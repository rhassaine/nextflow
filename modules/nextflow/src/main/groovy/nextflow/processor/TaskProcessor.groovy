/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.processor

import static nextflow.processor.ErrorStrategy.*

import java.lang.reflect.InvocationTargetException
import java.nio.file.FileSystems
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.LongAdder
import java.util.regex.Matcher
import java.util.regex.Pattern

import ch.artecat.grengine.Grengine
import com.google.common.hash.HashCode
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import groovyx.gpars.agent.Agent
import groovyx.gpars.dataflow.Dataflow
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.expression.DataflowExpression
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowOperator
import groovyx.gpars.dataflow.operator.DataflowProcessor
import groovyx.gpars.dataflow.operator.PoisonPill
import groovyx.gpars.dataflow.stream.DataflowStreamWriteAdapter
import groovyx.gpars.group.PGroup
import nextflow.NF
import nextflow.Nextflow
import nextflow.Session
import nextflow.ast.TaskCmdXform
import nextflow.ast.TaskTemplateVarsXform
import nextflow.cloud.CloudSpotTerminationException
import nextflow.dag.NodeMarker
import nextflow.exception.FailedGuardException
import nextflow.exception.IllegalArityException
import nextflow.exception.MissingFileException
import nextflow.exception.MissingValueException
import nextflow.exception.ProcessEvalException
import nextflow.exception.ProcessException
import nextflow.exception.ProcessFailedException
import nextflow.exception.ProcessRetryableException
import nextflow.exception.ProcessSubmitTimeoutException
import nextflow.exception.ProcessUnrecoverableException
import nextflow.exception.ShowOnlyExceptionMessage
import nextflow.exception.UnexpectedException
import nextflow.executor.CachedTaskHandler
import nextflow.executor.Executor
import nextflow.executor.StoredTaskHandler
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.file.FileHelper
import nextflow.file.FileHolder
import nextflow.file.FilePatternSplitter
import nextflow.file.FilePorter
import nextflow.file.LogicalDataPath
import nextflow.plugin.Plugins
import nextflow.processor.tip.TaskTipProvider
import nextflow.script.BaseScript
import nextflow.script.BodyDef
import nextflow.script.ProcessConfig
import nextflow.script.ScriptMeta
import nextflow.script.ScriptType
import nextflow.script.bundle.ResourcesBundle
import nextflow.script.params.BaseOutParam
import nextflow.script.params.CmdEvalParam
import nextflow.script.params.DefaultOutParam
import nextflow.script.params.EachInParam
import nextflow.script.params.EnvInParam
import nextflow.script.params.EnvOutParam
import nextflow.script.params.FileInParam
import nextflow.script.params.FileOutParam
import nextflow.script.params.InParam
import nextflow.script.params.MissingParam
import nextflow.script.params.OptionalParam
import nextflow.script.params.OutParam
import nextflow.script.params.StdInParam
import nextflow.script.params.StdOutParam
import nextflow.script.params.TupleInParam
import nextflow.script.params.TupleOutParam
import nextflow.script.params.ValueInParam
import nextflow.script.params.ValueOutParam
import nextflow.trace.TraceRecord
import nextflow.util.ArrayBag
import nextflow.util.BlankSeparatedList
import nextflow.util.CacheHelper
import nextflow.util.Escape
import nextflow.util.HashBuilder
import nextflow.util.LockManager
import nextflow.util.LoggerHelper
import nextflow.util.TestOnly
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer
/**
 * Implement nextflow process execution logic
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class TaskProcessor {

    static enum RunType {
        SUBMIT('Submitted process'),
        RETRY('Re-submitted process')

        String message;

        RunType(String str) { message=str };
    }

    static final public String TASK_CONTEXT_PROPERTY_NAME = 'task'

    final private static Pattern ENV_VAR_NAME = ~/[a-zA-Z_]+[a-zA-Z0-9_]*/

    final private static Pattern QUESTION_MARK = ~/(\?+)/

    @TestOnly private static volatile TaskProcessor currentProcessor0

    @TestOnly static TaskProcessor currentProcessor() { currentProcessor0 }

    /**
     * Keeps track of the task instance executed by the current thread
     */
    protected final ThreadLocal<TaskRun> currentTask = new ThreadLocal<>()

    /**
     * Unique task index number (run)
     */
    final protected AtomicInteger indexCount = new AtomicInteger()

    /**
     * The current workflow execution session
     */
    protected Session session

    /**
     * The script object which defines this task
     */
    protected BaseScript ownerScript

    /**
     * The processor descriptive name
     */
    protected String name

    /**
     * The piece of code to be execute provided by the user
     */
    protected BodyDef taskBody

    /**
     * The corresponding {@code DataflowProcessor} which will receive and
     * manage accordingly the task inputs
     *
     * note: it must be declared volatile -- issue #41
     */
    protected volatile DataflowProcessor operator

    /**
     * The underlying executor which will run the task
     */
    protected Executor executor

    /**
     * The corresponding task configuration properties, it holds the inputs/outputs
     * definition as well as other execution meta-declaration
     */
    protected ProcessConfig config

    /**
     * Count the number of time an error occurred
     */
    private volatile int errorCount


    /**
     * Set to true the very first time the error is shown.
     *
     * Note: it is declared static because the error must be shown only the
     * very first time  for all processes
     */
    private static final AtomicBoolean errorShown = new AtomicBoolean()

    /**
     * Flag set {@code true} when the processor termination has been invoked
     *
     * See {@code #checkProcessTermination}
     */
    protected volatile boolean completed

    protected boolean allScalarValues

    protected boolean hasEachParams

    /**
     * The state is maintained by using an agent
     */
    protected Agent<StateObj> state

    /**
     * Groovy engine used to evaluate dynamic code
     */
    protected Grengine grengine

    /**
     * Whenever the process is executed only once
     */
    protected boolean singleton

    /**
     * Track the status of input ports. When 1 the port is open (waiting for data),
     * when 0 the port is closed (ie. received the STOP signal)
     */
    protected AtomicIntegerArray openPorts

    /**
     * Process ID number. The first is 1, the second 2 and so on ..
     */
    private final int id

    private LongAdder forksCount

    private int maxForks

    private static int processCount

    private static LockManager lockManager = new LockManager()

    private List<Map<Short,List>> fairBuffers = new ArrayList<>()

    private int currentEmission

    private Boolean isFair0

    private TaskArrayCollector arrayCollector

    private CompilerConfiguration compilerConfig() {
        final config = new CompilerConfiguration()
        config.addCompilationCustomizers( new ASTTransformationCustomizer(TaskTemplateVarsXform) )
        config.addCompilationCustomizers( new ASTTransformationCustomizer(TaskCmdXform) )
        return config
    }

    @TestOnly
    static void reset() {
        processCount=0
        errorShown.set(false)
        currentProcessor0 = null
    }

    /*
     * Initialise the process ID
     *
     * Note: processes are create in a sequential manner (by the main thread that parse the script)
     * so it does not require a synchronized block
     */
    {
        id = ++processCount
        grengine = session && session.classLoader ? new Grengine(session.classLoader, compilerConfig()) : new Grengine(compilerConfig())
        currentProcessor0 = this
    }

    @TestOnly
    protected TaskProcessor() {}

    /**
     * Create and initialize the processor object
     *
     * @param name
     * @param executor
     * @param session
     * @param script
     * @param config
     * @param taskBody
     */
    TaskProcessor(String name, Executor executor, Session session, BaseScript script, ProcessConfig config, BodyDef taskBody ) {
        assert executor
        assert session
        assert script
        assert taskBody

        this.executor = executor
        this.session = session
        this.ownerScript = script
        this.config = config
        this.taskBody = taskBody
        this.name = name
        this.maxForks = config.maxForks && config.maxForks>0 ? config.maxForks as int : 0
        this.forksCount = maxForks ? new LongAdder() : null
        this.isFair0 = config.getFair()
        final arraySize = config.getArray()
        this.arrayCollector = arraySize > 0 ? new TaskArrayCollector(this, executor, arraySize) : null
        log.debug "Creating process '$name': maxForks=${maxForks}; fair=${isFair0}; array=${arraySize}"
    }

    /**
     * @return The processor unique id
     */
    int getId() { id }
  
    /**
     * @return The {@code TaskConfig} object holding the task configuration properties
     */
    ProcessConfig getConfig() { config }

    /**
     * @return The current {@code Session} instance
     */
    Session getSession() { session }

    /**
     * @return The processor name
     */
    String getName() { name }

    /**
     * @return The {@link Executor} associated to this processor
     */
    Executor getExecutor() { executor }

    /**
     * @return The {@code DataflowOperator} underlying this process
     */
    DataflowProcessor getOperator() { operator }

    /**
     * @return The {@code BaseScript} object which represents pipeline script
     */
    BaseScript getOwnerScript() { ownerScript }

    /**
     *  Define the type of script hold by the {@code #code} property
     */
    protected ScriptType getScriptType() { taskBody.type }

    /**
     * @return The user provided script block
     */
    BodyDef getTaskBody() { taskBody }

    Set<String> getDeclaredNames() {
        Set<String> result = new HashSet<>(20)
        result.addAll(config.getInputs().getNames())
        result.addAll(config.getOutputs().getNames())
        return result
    }

    LongAdder getForksCount() { forksCount }

    int getMaxForks() { maxForks }

    boolean hasErrors() { errorCount>0 }

    @Memoized
    protected TaskTipProvider getTipProvider() {
        final provider = Plugins.getPriorityExtensions(TaskTipProvider).find(it-> it.enabled())
        if( !provider )
            throw new IllegalStateException("Unable to find any tip provider")
        return provider
    }

    boolean isSingleton() { singleton }

    /**
     * Create a "preview" for a task run. This method is only meant for the creation of "mock" task run
     * to allow the access for the associated {@link TaskConfig} during a pipeline "preview" execution.
     *
     * Note this returns an "eventually" task configuration object. Also Inputs and output parameters are NOT
     * resolved by this method.
     *
     * @return A {@link TaskRun} object holding a reference to the associated {@link TaskConfig}
     */
    TaskRun createTaskPreview() {
        final task = new TaskRun(
                processor: this,
                type: scriptType,
                config: config.createTaskConfig(),
                context: new TaskContext(this)
        )
        task.config.context = task.context
        task.config.process = task.processor.name
        task.config.executor = task.processor.executor.name

        return task
    }

    protected void checkWarn(String msg, Map opts=null) {
        if( NF.isStrictMode() )
            throw new ProcessUnrecoverableException(msg)
        if( opts )
            log.warn1(opts, msg)
        else
            log.warn(msg)
    }

    /**
     * Launch the 'script' define by the code closure as a local bash script
     *
     * @param code A {@code Closure} returning a bash script e.g.
     *          <pre>
     *              {
     *                 """
     *                 #!/bin/bash
     *                 do this ${x}
     *                 do that ${y}
     *                 :
     *                 """
     *              }
     *
     * @return {@code this} instance
     */
    def run() {

        // -- check that the task has a body
        if ( !taskBody )
            throw new IllegalStateException("Missing task body for process `$name`")

        // -- check that input tuple defines at least two elements
        def invalidInputTuple = config.getInputs().find { it instanceof TupleInParam && it.inner.size()<2 }
        if( invalidInputTuple )
            checkWarn "Input `tuple` must define at least two elements -- Check process `$name`"

        // -- check that output tuple defines at least two elements
        def invalidOutputTuple = config.getOutputs().find { it instanceof TupleOutParam && it.inner.size()<2 }
        if( invalidOutputTuple )
            checkWarn "Output `tuple` must define at least two elements -- Check process `$name`"

        /**
         * Verify if this process run only one time
         */
        allScalarValues = config.getInputs().allScalarInputs()
        hasEachParams = config.getInputs().any { it instanceof EachInParam }

        /*
         * Normalize input channels
         */
        config.fakeInput()

        /*
         * Normalize the output
         * - even though the output may be empty, let return the stdout as output by default
         */
        if ( config.getOutputs().size() == 0 ) {
            config.fakeOutput()
        }

        // the state agent
        state = new Agent<>(new StateObj(name))
        state.addListener { StateObj old, StateObj obj ->
            try {
                log.trace "<$name> Process state changed to: $obj -- finished: ${obj.isFinished()}"
                if( !completed && obj.isFinished() ) {
                    terminateProcess()
                    completed = true
                }
            }
            catch( Throwable e ) {
                session.abort(e)
            }
        }

        // register the processor
        // note: register the task *before* creating (and starting the dataflow operator) in order
        // a race condition on the processes barrier - this fix issue #43
        session.processRegister(this)

        // create the underlying dataflow operator
        createOperator()

        session.notifyProcessCreate(this)

        /*
         * When there is a single output channel, return let returns that item
         * otherwise return the list
         */
        def result = config.getOutputs().channels
        return result.size() == 1 ? result[0] : result
    }

    /**
     * Template method which extending classes have to override in order to
     * create the underlying *dataflow* operator associated with this processor
     *
     * See {@code DataflowProcessor}
     */

    protected void createOperator() {
        def opInputs = new ArrayList(config.getInputs().getChannels())

        /*
         * check if there are some iterators declaration
         * the list holds the index in the list of all *inputs* for the {@code each} declaration
         */
        List<Integer> iteratorIndexes = []
        config.getInputs().eachWithIndex { param, index ->
            if( param instanceof EachInParam ) {
                log.trace "Process ${name} > got each param: ${param.name} at index: ${index} -- ${param.dump()}"
                iteratorIndexes << index
            }
        }

        /**
         * The thread pool used by GPars. The thread pool to be used is set in the static
         * initializer of {@link nextflow.cli.CmdRun} class. See also {@link nextflow.util.CustomPoolFactory}
         */
        final PGroup group = Dataflow.retrieveCurrentDFPGroup()

        /*
         * When one (or more) {@code each} are declared as input, it is created an extra
         * operator which will receive the inputs from the channel (excepts the values over iterate)
         *
         * The operator will *expand* the received inputs, iterating over the user provided value and
         * forwarding the final values the the second *parallel* processor executing the user specified task
         */
        if( iteratorIndexes ) {
            log.debug "Creating *combiner* operator for each param(s) at index(es): ${iteratorIndexes}"

            // don't care about the last channel, being the control channel it doesn't bring real values
            final size = opInputs.size()-1

            // the iterator operator needs to executed just one time
            // thus add a dataflow queue binding a single value and then a stop signal
            def termination = new DataflowQueue<>()
            termination << Boolean.TRUE
            opInputs[size] = termination

            // the channel forwarding the data from the *iterator* process to the target task
            final linkingChannels = new ArrayList(size)
            size.times { linkingChannels[it] = new DataflowQueue() }

            // the script implementing the iterating process
            final forwarder = new ForwardClosure(size, iteratorIndexes)

            // instantiate the iteration process
            def DataflowOperator op1
            def stopAfterFirstRun = allScalarValues
            def interceptor = new BaseProcessInterceptor(opInputs, stopAfterFirstRun)
            def params = [inputs: opInputs, outputs: linkingChannels, maxForks: 1, listeners: [interceptor]]
            session.allOperators << (op1 = new DataflowOperator(group, params, forwarder))
            // fix issue #41
            start(op1)

            // set as next inputs the result channels of the iteration process
            // adding the 'control' channel removed previously
            opInputs = new ArrayList(size+1)
            opInputs.addAll( linkingChannels )
            opInputs.add( config.getInputs().getChannels().last() )
        }

        /*
         * finally create the operator
         */
        // note: do not specify the output channels in the operator declaration
        // this allows us to manage them independently from the operator life-cycle
        this.singleton = allScalarValues && !hasEachParams
        this.openPorts = createPortsArray(opInputs.size())
        config.getOutputs().setSingleton(singleton)
        def interceptor = new TaskProcessorInterceptor(opInputs, singleton)
        def params = [inputs: opInputs, maxForks: session.poolSize, listeners: [interceptor] ]
        def invoke = new InvokeTaskAdapter(this, opInputs.size())
        session.allOperators << (operator = new DataflowOperator(group, params, invoke))

        // notify the creation of a new vertex the execution DAG
        NodeMarker.addProcessNode(this, config.getInputs(), config.getOutputs())

        // fix issue #41
        start(operator)
    }

    private start(DataflowProcessor op) {
        if( !NF.dsl2 ) {
            op.start()
            return
        }
        session.addIgniter {
            log.debug "Starting process > $name"
            op.start()
        }
    }

    private AtomicIntegerArray createPortsArray(int size) {
        def result = new AtomicIntegerArray(size)
        for( int i=0; i<size; i++ )
            result.set(i, 1)
        return result
    }

    /**
     * The processor execution body
     *
     * @param args
     *      The args array is expected to be composed by two elements:
     *      the first must be an object object of type {@link TaskStartParams},
     *      the second is the list of task input messages as received by the process
     */
    final protected void invokeTask( Object[] args ) {
        assert args.size()==2
        final params = (TaskStartParams) args[0]
        final values = (List) args[1]

        // create and initialize the task instance to be executed
        log.trace "Invoking task > $name with params=$params; values=$values"

        // -- create the task run instance
        final task = createTaskRun(params)
        // -- set the task instance as the current in this thread
        currentTask.set(task)

        // -- validate input lengths
        validateInputTuples(values)

        // -- map the inputs to a map and use to delegate closure values interpolation
        final secondPass = [:]
        int count = makeTaskContextStage1(task, secondPass, values)
        final foreignFiles = makeTaskContextStage2(task, secondPass, count)

        // verify that `when` guard, when specified, is satisfied
        if( !checkWhenGuard(task) )
            return

        // -- resolve the task command script
        task.resolve(taskBody)

        // -- verify if exists a stored result for this case,
        //    if true skip the execution and return the stored data
        if( checkStoredOutput(task) )
            return

        // -- download foreign files
        session.filePorter.transfer(foreignFiles)

        def hash = createTaskHashKey(task)
        checkCachedOrLaunchTask(task, hash, resumable)
    }

    @Memoized
    private List<TupleInParam> getDeclaredInputTuple() {
        getConfig().getInputs().ofType(TupleInParam)
    }

    protected void validateInputTuples( List values ) {

        def declaredSets = getDeclaredInputTuple()
        for( int i=0; i<declaredSets.size(); i++ ) {
            final param = declaredSets[i]
            final entry = values[param.index]
            final expected = param.inner.size()
            final actual = entry instanceof Collection ? entry.size() : (entry instanceof Map ? entry.size() : 1)

            if( actual != expected ) {
                final msg = "Input tuple does not match tuple declaration in process `$name` -- offending value: $entry"
                checkWarn(msg, [firstOnly: true, cacheKey: this])
            }
        }
    }


    /**
     * @return A string 'she-bang' formatted to the added on top script to be executed.
     * The interpreter to be used define by the *taskConfig* property {@code shell}
     */
    static String shebangLine(shell) {
        assert shell, "Missing 'shell' property in process configuration"

        String result = shell instanceof List ? shell.join(' ') : shell
        if( result.startsWith('/') ) {
            result = '#!' + result
        }
        else {
            result= '#!/usr/bin/env ' + result
        }

        return result

    }

    /**
     * Remove extra leading, trailing whitespace and newlines chars,
     * also if the script does not start with a {@code shebang} line,
     * add the default by using the current {@code #shell} attribute
     */
    static String normalizeScript(String script, shell) {
        assert script != null

        def result = new StringBuilder()
        result << script.stripIndent(true).trim()
        result << '\n'

        if( result[0] != '#' || result[1] != '!') {
            result.insert(0, shebangLine(shell) + '\n')
        }

        return result.toString()
    }

    /**
     * Given the task script extract the top *she-bang* interpreter declaration removing the {@code #!} characters.
     * @param script The script to be executed
     *
     * @return The interpreter as defined in the she-bang declaration, for example {@code /usr/bin/env perl}
     */
    static String fetchInterpreter( String script ) {

        if( script && script[0] == '#' && script[1] == '!') {
            return script.readLines()[0].substring(2)
        }

        return null
    }

    /**
     * Create a new {@code TaskRun} instance, initializing the following properties :
     * <li>{@code TaskRun#id}
     * <li>{@code TaskRun#status}
     * <li>{@code TaskRun#index}
     * <li>{@code TaskRun#name}
     * <li>{@code TaskRun#process}
     *
     * @return The new newly created {@code TaskRun}
     */

    final protected TaskRun createTaskRun(TaskStartParams params) {
        final task = new TaskRun(
                id: params.id,
                index: params.index,
                processor: this,
                type: scriptType,
                config: config.createTaskConfig(),
                context: new TaskContext(this)
        )

        // setup config
        task.config.index = task.index
        task.config.process = task.processor.name
        task.config.executor = task.processor.executor.name

        /*
         * initialize the inputs/outputs for this task instance
         */
        config.getInputs().each { InParam param ->
            if( param instanceof TupleInParam )
                param.inner.each { task.setInput(it)  }
            else if( param instanceof EachInParam )
                task.setInput(param.inner)
            else
                task.setInput(param)
        }

        config.getOutputs().each { OutParam param ->
            if( param instanceof TupleOutParam ) {
                param.inner.each { task.setOutput(it) }
            }
            else
                task.setOutput(param)
        }

        return task
    }

    /**
     * Try to check if exists a previously executed process result in the a cached folder. If it exists
     * use the that result and skip the process execution, otherwise the task is sumitted for execution.
     *
     * @param task
     *      The {@code TaskRun} instance to be executed
     * @param hash
     *      The unique {@code HashCode} for the given task inputs
     * @param script
     *      The script to be run (only when it's a merge task)
     * @return
     *      {@code false} when a cached result has been found and the execution has skipped,
     *      or {@code true} if the task has been submitted for execution
     *
     */
    @CompileStatic
    final protected void checkCachedOrLaunchTask( TaskRun task, HashCode hash, boolean shouldTryCache ) {

        int tries = task.failCount +1
        while( true ) {
            hash = HashBuilder.defaultHasher().putBytes(hash.asBytes()).putInt(tries).hash()

            Path resumeDir = null
            boolean exists = false
            try {
                final entry = session.cache.getTaskEntry(hash, this)
                resumeDir = entry ? FileHelper.asPath(entry.trace.getWorkDir()) : null
                if( resumeDir )
                    exists = resumeDir.exists()

                log.trace "[${safeTaskName(task)}] Cacheable folder=${resumeDir?.toUriString()} -- exists=$exists; try=$tries; shouldTryCache=$shouldTryCache; entry=$entry"
                final cached = shouldTryCache && exists && entry.trace.isCompleted() && checkCachedOutput(task.clone(), resumeDir, hash, entry)
                if( cached )
                    break
            }
            catch (Throwable t) {
                log.warn1("[${safeTaskName(task)}] Unable to resume cached task -- See log file for details", causedBy: t)
            }

            if( exists ) {
                tries++
                continue
            }

            final lock = lockManager.acquire(hash)
            final workDir = task.getWorkDirFor(hash)
            try {
                if( resumeDir != workDir )
                    exists = workDir.exists()
                if( exists ) {
                    tries++
                    continue
                }
                else if( !workDir.mkdirs() )
                    throw new IOException("Unable to create directory=$workDir -- check file system permissions")
            }
            finally {
                lock.release()
            }

            // submit task for execution
            submitTask( task, hash, workDir )
            break
        }

    }

    /**
     * Check if exists a *storeDir* for the specified task. When if exists
     * and contains the expected result files, the process execution is skipped.
     *
     * @param task The task for which check the stored output
     * @return {@code true} when the folder exists and it contains the expected outputs,
     *      {@code false} otherwise
     */
    final boolean checkStoredOutput( TaskRun task ) {
        if( !task.config.storeDir ) {
            log.trace "[${safeTaskName(task)}] storeDir not set -- return false"
            return false
        }

        // -- when store path is set, only output params of type 'file' can be specified
        final ctx = task.context
        def invalid = task.getOutputs().keySet().any {
            if( it instanceof ValueOutParam ) {
                return !ctx.containsKey(it.name)
            }
            if( it instanceof FileOutParam ) {
                return false
            }
            return true
        }
        if( invalid ) {
            checkWarn "[${safeTaskName(task)}] storeDir can only be used with `val` and `path` outputs"
            return false
        }

        if( !task.config.getStoreDir().exists() ) {
            log.trace "[${safeTaskName(task)}] storeDir does not exist > ${task.config.storeDir} -- return false"
            // no folder -> no cached result
            return false
        }


        try {
            // -- expose task exit status to make accessible as output value
            task.config.exitStatus = TaskConfig.EXIT_ZERO
            // -- check if all output resources are available
            collectOutputs(task)
            log.info "[skipping] Stored process > ${safeTaskName(task)}"
            // set the exit code in to the task object
            task.exitStatus = TaskConfig.EXIT_ZERO
            task.cached = true
            session.notifyTaskCached(new StoredTaskHandler(task))

            // -- now bind the results
            finalizeTask0(task)
            return true
        }
        catch( MissingFileException | MissingValueException e ) {
            log.trace "[${safeTaskName(task)}] Missed storeDir > ${e.getMessage()} -- folder: ${task.config.storeDir}"
            task.exitStatus = Integer.MAX_VALUE
            task.workDir = null
            return false
        }
    }

    /**
     * Check whenever the outputs for the specified task already exist
     *
     * @param task The task instance
     * @param folder The folder where the outputs are stored (eventually)
     * @return {@code true} when all outputs are available, {@code false} otherwise
     */
    final boolean checkCachedOutput(TaskRun task, Path folder, HashCode hash, TaskEntry entry) {

        // check if exists the task exit code file
        def exitCode = null
        def exitFile = folder.resolve(TaskRun.CMD_EXIT)
        if( task.type == ScriptType.SCRIPTLET ) {
            def str
            try {
                str = exitFile.text?.trim()
            }
            catch( IOException e ) {
                log.trace "[${safeTaskName(task)}] Exit file can't be read > $exitFile -- return false -- Cause: ${e.message}"
                return false
            }

            exitCode = str.isInteger() ? str.toInteger() : null
            if( !task.isSuccess(exitCode) ) {
                log.trace "[${safeTaskName(task)}] Exit code is not valid > $str -- return false"
                return false
            }
        }

        /*
         * verify cached context map
         */
        if( !entry ) {
            log.trace "[${safeTaskName(task)}] Missing cache entry -- return false"
            return false
        }

        if( task.hasCacheableValues() && !entry.context ) {
            log.trace "[${safeTaskName(task)}] Missing cache context -- return false"
            return false
        }

        /*
         * verify stdout file
         */
        final stdoutFile = folder.resolve( TaskRun.CMD_OUTFILE )

        if( entry.context != null ) {
            task.context = entry.context
            task.config.context = entry.context
            task.code?.delegate = entry.context
        }

        try {
            // -- expose task exit status to make accessible as output value
            task.config.exitStatus = exitCode
            // -- check if all output resources are available
            collectOutputs(task, folder, stdoutFile, task.context)

            // set the exit code in to the task object
            task.cached = true
            task.hash = hash
            task.workDir = folder
            task.stdout = stdoutFile
            if( exitCode != null ) {
                task.exitStatus = exitCode
            }

            log.info "[${task.hashLog}] Cached process > ${task.name}"
            // -- notify cached event
            if( entry )
                session.notifyTaskCached(new CachedTaskHandler(task,entry.trace))

            // -- now bind the results
            finalizeTask0(task)
            return true
        }
        catch( MissingFileException | MissingValueException e ) {
            log.trace "[${safeTaskName(task)}] Missed cache > ${e.getMessage()} -- folder: $folder"
            task.exitStatus = Integer.MAX_VALUE
            task.workDir = null
            return false
        }
    }

    /**
     * Handles an error raised during the processor execution
     *
     * @param error The exception raised during the task execution
     * @param task The {@code TaskDef} instance which raised the exception
     * @return {@code true} to terminate the processor execution,
     *         {@code false} ignore the error and continue to process other pending tasks
     */
    final protected boolean handleException( Throwable error, TaskRun task = null ) {
        log.trace "Handling error: $error -- task: $task"
        def fault = resumeOrDie(task, error)
        log.trace "Task fault (2): $fault"

        if (fault instanceof TaskFault) {
            session.fault(fault)
            // when a `TaskFault` is returned a `TERMINATE` is implicit, thus return `true`
            return true
        }

        return fault == TERMINATE || fault == FINISH
    }

    /**
     * @param task The {@code TaskRun} instance that raised an error
     * @param error The error object
     * @return
     *      Either a value of value of {@link ErrorStrategy} representing the error strategy chosen
     *      or an instance of {@TaskFault} representing the cause of the error (that implicitly means
     *      a {@link ErrorStrategy#TERMINATE})
     */
    @PackageScope
    final synchronized resumeOrDie( TaskRun task, Throwable error, TraceRecord traceRecord = null) {
        log.debug "Handling unexpected condition for\n  task: name=${safeTaskName(task)}; work-dir=${task?.workDirStr}\n  error [${error?.class?.name}]: ${error?.getMessage()?:error}"

        ErrorStrategy errorStrategy = TERMINATE
        final List<String> message = []
        try {
            // -- do not recoverable error, just re-throw it
            if( error instanceof Error ) throw error

            // -- retry without increasing the error counts
            if( task && (error.cause instanceof ProcessRetryableException || error.cause instanceof CloudSpotTerminationException) ) {
                if( error.cause instanceof ProcessRetryableException )
                    log.info "[$task.hashLog] NOTE: ${error.message} -- Execution is retried"
                else
                    log.info "[$task.hashLog] NOTE: ${error.message} -- Cause: ${error.cause.message} -- Execution is retried"
                task.failCount+=1
                final taskCopy = task.makeCopy()
                session.getExecService().submit {
                    try {
                        taskCopy.runType = RunType.RETRY
                        checkCachedOrLaunchTask( taskCopy, taskCopy.hash, false )
                    }
                    catch( Throwable e ) {
                        log.error("Unable to re-submit task `${taskCopy.name}`", e)
                        session.abort(e)
                    }
                }
                task.failed = true
                task.errorAction = RETRY
                return RETRY
            }

            final submitTimeout = error.cause instanceof ProcessSubmitTimeoutException
            final submitErrMsg = submitTimeout ? error.cause.message : null
            final int submitRetries = submitTimeout ? ++task.submitRetries : 0
            final int taskErrCount = !submitTimeout && task ? ++task.failCount : 0
            final int procErrCount = !submitTimeout ? ++errorCount : errorCount

            // -- when is a task level error and the user has chosen to ignore error,
            //    just report and error message and DO NOT stop the execution
            if( task && error instanceof ProcessException ) {
                // expose current task exit status
                task.config.exitStatus = task.exitStatus
                task.config.errorCount = procErrCount
                task.config.retryCount = taskErrCount
                //Add trace of the previous execution in the task context for next execution
                if ( traceRecord )
                    task.config.previousTrace = traceRecord
                task.config.previousException = error

                errorStrategy = checkErrorStrategy(task, error, taskErrCount, procErrCount, submitRetries)
                if( errorStrategy.soft ) {
                    def msg = "[$task.hashLog] NOTE: ${submitTimeout ? submitErrMsg : error.message}"
                    if( errorStrategy == IGNORE )
                        msg += " -- Error is ignored"
                    else if( errorStrategy == RETRY )
                        msg += " -- Execution is retried (${submitTimeout ? submitRetries : taskErrCount})"
                    log.info msg
                    task.failed = true
                    task.errorAction = errorStrategy
                    return errorStrategy
                }
            }

            // -- mark the task as failed
            if( task ) {
                task.failed = true
                task.errorAction = errorStrategy
            }

            // -- make sure the error is showed only the very first time across all processes
            if( errorShown.getAndSet(true) || session.aborted ) {
                log.trace "Task errorShown=${errorShown.get()}; aborted=${session.aborted}"
                return errorStrategy
            }

            def dumpStackTrace = log.isTraceEnabled()
            message << "Error executing process > '${safeTaskName(task)}'"
            switch( error ) {
                case ProcessException:
                    formatTaskError( message, error, task )
                    break

                case ProcessEvalException:
                    formatCommandError( message, error, task )
                    break

                case FailedGuardException:
                    formatGuardError( message, error as FailedGuardException, task )
                    break;

                default:
                    message << formatErrorCause(error)
                    dumpStackTrace = true
            }

            if( dumpStackTrace )
                log.error(message.join('\n'), error)
            else
                log.error(message.join('\n'))
        }
        catch( Throwable e ) {
            // no recoverable error
            log.error("Execution aborted due to an unexpected error", e )
        }

        return new TaskFault(error: error, task: task, report: message.join('\n'))
    }

    protected String safeTaskName(TaskRun task)  {
        return task!=null
                ? task.lazyName()
                : name
    }

    protected ErrorStrategy checkErrorStrategy( TaskRun task, ProcessException error, final int taskErrCount, final int procErrCount, final submitRetries ) {

        final action = task.config.getErrorStrategy()

        // retry is not allowed when the script cannot be compiled or similar errors
        if( error instanceof ProcessUnrecoverableException || error.cause instanceof ProcessUnrecoverableException ) {
            return !action.soft ? action : TERMINATE
        }

        // IGNORE strategy -- just continue
        if( action == IGNORE ) {
            return IGNORE
        }

        // RETRY strategy -- check that process do not exceed 'maxError' and the task do not exceed 'maxRetries'
        if( action == RETRY ) {
            final int maxErrors = task.config.getMaxErrors()
            final int maxRetries = task.config.getMaxRetries()

            if( (procErrCount < maxErrors || maxErrors == -1) && taskErrCount <= maxRetries && submitRetries <= maxRetries ) {
                final taskCopy = task.makeCopy()
                session.getExecService().submit({
                    try {
                        taskCopy.config.attempt = taskErrCount+1
                        taskCopy.config.submitAttempt = submitRetries+1
                        taskCopy.runType = RunType.RETRY
                        taskCopy.resolve(taskBody)
                        checkCachedOrLaunchTask( taskCopy, taskCopy.hash, false )
                    }
                    catch( Throwable e ) {
                        log.error("Unable to re-submit task `${safeTaskName(taskCopy)}`", e)
                        session.abort(e)
                    }
                } as Runnable)
                return RETRY
            }

            return TERMINATE
        }

        return action
    }

    final protected List<String> formatCommandError(List<String> message, ProcessEvalException error, TaskRun task) {
        // compose a readable error message
        message << formatErrorCause(error)

        // - print the executed command
        message << "Command executed:\n"
        error.command.stripIndent(true)?.trim()?.eachLine {
            message << "  ${it}"
        }

        // - the exit status
        message << "\nCommand exit status:\n  ${error.status}"

        // - the tail of the process stdout
        message << "\nCommand output:"
        def lines = error.output.readLines()
        if( lines.size() == 0 ) {
            message << "  (empty)"
        }
        for( String it : lines ) {
            message << "  ${stripWorkDir(it, task.workDir)}"
        }

        if( task?.workDir )
            message << "\nWork dir:\n  ${task.workDirStr}"

        return message
    }

    final protected List<String> formatGuardError( List<String> message, FailedGuardException error, TaskRun task ) {
        // compose a readable error message
        message << formatErrorCause(error)

        if( error.source )  {
            message << "\nWhen block:"
            error.source.stripIndent(true).eachLine {
                message << "  $it"
            }
        }

        if( task?.workDir )
            message << "\nWork dir:\n  ${task.workDirStr}"

        return message
    }

    final protected List<String> formatTaskError( List<String> message, Throwable error, TaskRun task ) {

        // compose a readable error message
        message << formatErrorCause( error )

        /*
         * task executing scriptlets
         */
        if( task?.script ) {
            // - print the executed command
            message << "Command executed${task.template ? " [$task.template]": ''}:\n"
            task.script?.stripIndent(true)?.trim()?.eachLine {
                message << "  ${it}"
            }

            // - the exit status
            message << "\nCommand exit status:\n  ${task.exitStatus != Integer.MAX_VALUE ? task.exitStatus : '-'}"

            // - the tail of the process stdout
            message << "\nCommand output:"
            final max = 50
            def lines = task.dumpStdout(max)
            if( lines.size() == 0 ) {
                message << "  (empty)"
            }
            for( String it : lines ) {
                message << "  ${stripWorkDir(it, task.workDir)}"
            }

            // - the tail of the process stderr
            lines = task.dumpStderr(max)
            if( lines ) {
                message << "\nCommand error:"
                for( String it : lines ) {
                    message << "  ${stripWorkDir(it, task.workDir)}"
                }
            }
            // - this is likely a task wrapper issue
            else if( task.exitStatus != 0 ) {
                lines = task.dumpLogFile(max)
                if( lines ) {
                    message << "\nCommand wrapper:"
                    for( String it : lines ) {
                        message << "  ${stripWorkDir(it, task.workDir)}"
                    }
                }
            }

        }
        else {
            if( task?.source )  {
                message << "Source block:"
                task.source.stripIndent(true).eachLine {
                    message << "  $it"
                }
            }

        }

        if( task?.workDir )
            message << "\nWork dir:\n  ${task.workDirStr}"

        if( task?.isContainerEnabled() )
            message << "\nContainer:\n  ${task.container}".toString()

        message << suggestTip(message)

        return message
    }

    private String suggestTip(List<String> message) {
        try {
            return "\nTip: ${getTipProvider().suggestTip(message)}"
        }
        catch (Exception e) {
            log.debug "Unable to get tip for task message: $message", e
            return ''
        }
    }

    private static String stripWorkDir(String line, Path workDir) {
        if( workDir==null ) return line
        if( workDir.fileSystem != FileSystems.default ) return line
        return workDir ? line.replace(workDir.toString()+'/','') : line
    }


    /**
     * Send a poison pill over all the outputs channel
     */
    final protected synchronized void sendPoisonPill() {
        log.trace "<$name> Sending a poison pill(s)"

        for( DataflowWriteChannel channel : config.getOutputs().getChannels() ){

            if( channel instanceof DataflowQueue ) {
                channel.bind( PoisonPill.instance )
            }
            else if( channel instanceof DataflowStreamWriteAdapter ) {
                channel.bind( PoisonPill.instance )
            }
            else if( channel instanceof DataflowExpression && !channel.isBound()) {
                channel.bind( PoisonPill.instance )
            }
        }
    }

    private String formatErrorCause( Throwable error ) {

        def result = new StringBuilder()
        result << '\nCaused by:\n'

        def message
        if( error instanceof ShowOnlyExceptionMessage || !error.cause )
            message = err0(error)
        else
            message = err0(error.cause)

        for( String line : message.readLines() ) {
            result << '  ' << line << '\n'
        }

        result
            .append('\n')
            .toString()
    }


    static String err0(Throwable e) {
        final fail = e instanceof InvocationTargetException ? e.targetException : e

        if( fail instanceof NoSuchFileException ) {
            return "No such file or directory: $fail.message"
        }
        if( fail instanceof MissingPropertyException ) {
            def name = fail.property ?: LoggerHelper.getDetailMessage(fail)
            def result = "No such variable: ${name}"
            def details = LoggerHelper.findErrorLine(fail)
            if( details )
                result += " -- Check script '${details[0]}' at line: ${details[1]}"
            return result
        }
        def result = fail.message ?: fail.toString()
        def details = LoggerHelper.findErrorLine(fail)
        if( details ){
            result += " -- Check script '${details[0]}' at line: ${details[1]}"
        }
        return result
    }

    /**
     * Publish output files to a specified target folder
     *
     * @param task The task whose outputs need to be published
     * @param overwrite When {@code true} any existing file will be overwritten, otherwise the publishing is ignored
     */
    @CompileStatic
    protected void publishOutputs( TaskRun task ) {
        final publishList = task.config.getPublishDir()
        if( !publishList ) {
            return
        }

        for( PublishDir pub : publishList ) {
            publishOutputs0(task, pub)
        }
    }

    private void publishOutputs0( TaskRun task, PublishDir publish ) {

        if( publish.overwrite == null ) {
            publish.overwrite = !task.cached
        }

        HashSet<Path> files = []
        def outputs = task.getOutputsByType(FileOutParam)
        for( Map.Entry entry : outputs ) {
            final value = entry.value
            if( value instanceof Path ) {
                files.add((Path)value)
            }
            else if( value instanceof Collection<Path> ) {
                files.addAll(value)
            }
            else if( value != null ) {
                throw new IllegalArgumentException("Unknown output file object [${value.class.name}]: ${value}")
            }
        }

        publish.apply(files, task)
    }

    /**
     * Bind the expected output files to the corresponding output channels
     * @param processor
     */
    synchronized protected void bindOutputs( TaskRun task ) {

        // -- creates the map of all tuple values to bind
        Map<Short,List> tuples = [:]
        for( OutParam param : config.getOutputs() ) {
            tuples.put(param.index, [])
        }

        // -- collects the values to bind
        for( OutParam param: task.outputs.keySet() ){
            def value = task.outputs.get(param)

            switch( param ) {
            case StdOutParam:
                log.trace "Process $name > normalize stdout param: $param"
                value = value instanceof Path ? value.text : value?.toString()

            case OptionalParam:
                if( !value && param instanceof OptionalParam && param.optional ) {
                    final holder = [] as MissingParam; holder.missing = param
                    tuples[param.index] = holder
                    break
                }

            case EnvOutParam:
            case ValueOutParam:
            case DefaultOutParam:
                log.trace "Process $name > collecting out param: ${param} = $value"
                tuples[param.index].add(value)
                break

            default:
                throw new IllegalArgumentException("Illegal output parameter type: $param")
            }
        }

        // bind the output
        if( isFair0 ) {
            fairBindOutputs0(tuples, task)
        }
        else {
            bindOutputs0(tuples)
        }

        // -- finally prints out the task output when 'debug' is true
        if( task.config.debug ) {
            task.echoStdout(session)
        }
    }

    protected void fairBindOutputs0(Map<Short,List> emissions, TaskRun task) {
        synchronized (isFair0) {
            // decrement -1 because tasks are 1-based
            final index = task.index-1
            // store the task emission values in a buffer
            fairBuffers[index-currentEmission] = emissions
            // check if the current task index matches the expected next emission index
            if( currentEmission == index ) {
                while( emissions!=null ) {
                    // bind the emission values
                    bindOutputs0(emissions)
                    // remove the head and try with the following
                    fairBuffers.remove(0)
                    // increase the index of the next emission
                    currentEmission++
                    // take the next emissions 
                    emissions = fairBuffers[0]
                }
            }
        }
    }

    protected void bindOutputs0(Map<Short,List> tuples) {
        // -- bind out the collected values
        for( OutParam param : config.getOutputs() ) {
            final outValue = tuples[param.index]
            if( outValue == null )
                throw new IllegalStateException()

            if( outValue instanceof MissingParam ) {
                log.debug "Process $name > Skipping output binding because one or more optional files are missing: $outValue.missing"
                continue
            }

            log.trace "Process $name > Binding out param: ${param} = ${outValue}"
            bindOutParam(param, outValue)
        }
    }

    protected void bindOutParam( OutParam param, List values ) {
        log.trace "<$name> Binding param $param with $values"
        final x = values.size() == 1 ? values[0] : values
        final ch = param.getOutChannel()
        if( ch != null ) {
            // create a copy of the output list of operation made by a downstream task
            // can modify the list which is used internally by the task processor
            // and result in a potential error. See https://github.com/nextflow-io/nextflow/issues/3768
            final copy = x instanceof List && x instanceof Cloneable ? x.clone() : x
            // emit the final value
            ch.bind(copy)
        }
    }

    protected void collectOutputs( TaskRun task ) {
        collectOutputs( task, task.getTargetDir(), task.@stdout, task.context )
    }

    /**
     * Once the task has completed this method is invoked to collected all the task results
     *
     * @param task
     */
    final protected void collectOutputs( TaskRun task, Path workDir, def stdout, Map context ) {
        log.trace "<$name> collecting output: ${task.outputs}"

        for( OutParam param : task.outputs.keySet() ) {

            switch( param ) {
                case StdOutParam:
                    collectStdOut(task, (StdOutParam)param, stdout)
                    break

                case FileOutParam:
                    collectOutFiles(task, (FileOutParam)param, workDir, context)
                    break

                case ValueOutParam:
                    collectOutValues(task, (ValueOutParam)param, context)
                    break

                case EnvOutParam:
                    collectOutEnvParam(task, (EnvOutParam)param, workDir)
                    break

                case CmdEvalParam:
                    collectOutEnvParam(task, (CmdEvalParam)param, workDir)
                    break

                case DefaultOutParam:
                    task.setOutput(param, DefaultOutParam.Completion.DONE)
                    break

                default:
                    throw new IllegalArgumentException("Illegal output parameter: ${param.class.simpleName}")

            }
        }

        // mark ready for output binding
        task.canBind = true
    }

    protected void collectOutEnvParam(TaskRun task, BaseOutParam param, Path workDir) {

        // fetch the output value
        final outCmds =  param instanceof CmdEvalParam ? task.getOutputEvals() : null
        final val = collectOutEnvMap(workDir,outCmds).get(param.name)
        if( val == null && !param.optional )
            throw new MissingValueException("Missing environment variable: $param.name")
        // set into the output set
        task.setOutput(param,val)
        // trace the result
        log.trace "Collecting param: ${param.name}; value: ${val}"

    }

    /**
     * Parse the `.command.env` file which holds the value for `env` and `cmd`
     * output types
     *
     * @param workDir
     *      The task work directory that contains the `.command.env` file
     * @param outEvals
     *      A {@link Map} instance containing key-value pairs
     * @return
     */
    @CompileStatic
    @Memoized(maxCacheSize = 10_000)
    protected Map collectOutEnvMap(Path workDir, Map<String,String> outEvals) {
        final env = workDir.resolve(TaskRun.CMD_ENV).text
        final result = new HashMap<String,String>(50)
        Matcher matcher
        // `current` represent the current capturing env variable name
        String current=null
        for(String line : env.readLines() ) {
            // Opening condition:
            // line should match a KEY=VALUE syntax
            if( !current && (matcher = (line=~/([a-zA-Z_][a-zA-Z0-9_]*)=(.*)/)) ) {
                final k = matcher.group(1)
                final v = matcher.group(2)
                if (!k) continue
                result.put(k,v)
                current = k
            }
            // Closing condition:
            // line should match /KEY/  or  /KEY/=exit_status
            else if( current && (matcher = (line=~/\/${current}\/(?:=exit:(\d+))?/)) ) {
                final status = matcher.group(1) as Integer ?: 0
                // when exit status is defined and it is a non-zero, it should be interpreted
                // as a failure of the execution of the output command; in this case the variable
                // holds the std error message
                if( outEvals!=null && status ) {
                    final cmd = outEvals.get(current)
                    final out = result[current]
                    throw new ProcessEvalException("Unable to evaluate output", cmd, out, status)
                }
                // reset current key
                current = null
            }
            else if( current && line!=null) {
                result[current] += '\n' + line
            }
        }
        return result
    }

    /**
     * Collects the process 'std output'
     *
     * @param task The executed process instance
     * @param param The declared {@link StdOutParam} object
     * @param stdout The object holding the task produced std out object
     */
    protected void collectStdOut( TaskRun task, StdOutParam param, def stdout ) {

        if( stdout == null && task.type == ScriptType.SCRIPTLET ) {
            throw new IllegalArgumentException("Missing 'stdout' for process > ${safeTaskName(task)}")
        }

        if( stdout instanceof Path && !stdout.exists() ) {
            throw new MissingFileException("Missing 'stdout' file: ${stdout.toUriString()} for process > ${safeTaskName(task)}")
        }

        task.setOutput(param, stdout)
    }

    protected void collectOutFiles( TaskRun task, FileOutParam param, Path workDir, Map context ) {

        final List<Path> allFiles = []
        // type file parameter can contain a multiple files pattern separating them with a special character
        def entries = param.getFilePatterns(context, task.workDir)
        boolean inputsRemovedFlag = false
        // for each of them collect the produced files
        for( String filePattern : entries ) {
            List<Path> result = null

            def splitter = param.glob ? FilePatternSplitter.glob().parse(filePattern) : null
            if( splitter?.isPattern() ) {
                result = fetchResultFiles(param, filePattern, workDir)
                // filter the inputs
                if( result && !param.includeInputs ) {
                    result = filterByRemovingStagedInputs(task, result, workDir)
                    log.trace "Process ${safeTaskName(task)} > after removing staged inputs: ${result}"
                    inputsRemovedFlag |= (result.size()==0)
                }
            }
            else {
                def path = param.glob ? splitter.strip(filePattern) : filePattern
                def file = workDir.resolve(path)
                def exists = checkFileExists(file, param.followLinks)
                if( exists )
                    result = List.of(file)
                else
                    log.debug "Process `${safeTaskName(task)}` is unable to find [${file.class.simpleName}]: `$file` (pattern: `$filePattern`)"
            }

            if( result )
                allFiles.addAll(result)

            else if( !param.optional && (!param.arity || param.arity.min > 0) ) {
                def msg = "Missing output file(s) `$filePattern` expected by process `${safeTaskName(task)}`"
                if( inputsRemovedFlag )
                    msg += " (note: input files are not included in the default matching set)"
                throw new MissingFileException(msg)
            }
        }

        if( !param.isValidArity(allFiles.size()) )
            throw new IllegalArityException("Incorrect number of output files for process `${safeTaskName(task)}` -- expected ${param.arity}, found ${allFiles.size()}")

        task.setOutput( param, allFiles.size()==1 && param.isSingle() ? allFiles[0] : allFiles )

    }

    protected boolean checkFileExists(Path file, boolean followLinks) {
        followLinks ? file.exists() : file.exists(LinkOption.NOFOLLOW_LINKS)
    }

    protected void collectOutValues( TaskRun task, ValueOutParam param, Map ctx ) {

        try {
            // fetch the output value
            final val = param.resolve(ctx)
            // set into the output set
            task.setOutput(param,val)
            // trace the result
            log.trace "Collecting param: ${param.name}; value: ${val}"
        }
        catch( MissingPropertyException e ) {
            throw new MissingValueException("Missing value declared as output parameter: ${e.property}")
        }

    }

    /**
     * Collect the file(s) with the name specified, produced by the execution
     *
     * @param workDir The job working path
     * @param namePattern The file name, it may include file name wildcards
     * @return The list of files matching the specified name in lexicographical order
     * @throws MissingFileException when no matching file is found
     */
    @PackageScope
    List<Path> fetchResultFiles( FileOutParam param, String namePattern, Path workDir ) {
        assert namePattern
        assert workDir

        List<Path> files = []
        def opts = visitOptions(param, namePattern)
        // scan to find the file with that name
        try {
            FileHelper.visitFiles(opts, workDir, namePattern) { Path it -> files.add(it) }
        }
        catch( NoSuchFileException e ) {
            throw new MissingFileException("Cannot access directory: '$workDir'", e)
        }

        return files.sort()
    }

    /**
     * Given a {@link FileOutParam} object create the option map for the
     * {@link FileHelper#visitFiles(java.util.Map, java.nio.file.Path, java.lang.String, groovy.lang.Closure)} method
     *
     * @param param A task {@link FileOutParam}
     * @param namePattern A file glob pattern
     * @return A {@link Map} object holding the traverse options for the {@link FileHelper#visitFiles(java.util.Map, java.nio.file.Path, java.lang.String, groovy.lang.Closure)} method
     */
    @PackageScope
    Map visitOptions( FileOutParam param, String namePattern ) {
        final opts = [:]
        opts.relative = false
        opts.hidden = param.hidden ?: namePattern.startsWith('.')
        opts.followLinks = param.followLinks
        opts.maxDepth = param.maxDepth
        opts.type = param.type ? param.type : ( namePattern.contains('**') ? 'file' : 'any' )
        return opts
    }

    /**
     * Given a list of {@code Path} removes all the hidden file i.e. the ones which names starts with a dot char
     * @param files A list of {@code Path}
     * @return The result list not containing hidden file entries
     */
    @PackageScope
    List<Path> filterByRemovingHiddenFiles( List<Path> files ) {
        files.findAll { !it.getName().startsWith('.') }
    }

    /**
     * Given a list of {@code Path} removes all the entries which name match the name of
     * file used as input for the specified {@code TaskRun}
     *
     * See TaskRun#getStagedInputs
     *
     * @param task
     *      A {@link TaskRun} object representing the task executed
     * @param collectedFiles
     *      Collection of candidate output files
     * @return
     *      List of the actual output files (not including any input matching an output file name pattern)
     */
    @PackageScope
    List<Path> filterByRemovingStagedInputs( TaskRun task, List<Path> collectedFiles, Path workDir ) {

        // get the list of input files
        final List<String> allStaged = task.getStagedInputs()
        final List<Path> result = new ArrayList<>(collectedFiles.size())

        for( int i=0; i<collectedFiles.size(); i++ ) {
            final it = collectedFiles.get(i)
            final relName = workDir.relativize(it).toString()
            if( !allStaged.contains(relName) )
                result.add(it)
        }

        return result
    }

    @Memoized
    ResourcesBundle getModuleBundle() {
        final script = this.getOwnerScript()
        final meta = ScriptMeta.get(script)
        return meta?.isModule() ? meta.getModuleBundle() : null
    }

    @Memoized
    protected List<Path> getBinDirs() {
        final result = new ArrayList(10)
        // module bundle bin dir have priority, add before
        final bundle = session.enableModuleBinaries() ? getModuleBundle() : null
        if( bundle!=null )
            result.addAll(bundle.getBinDirs())
        // then add project bin dir
        if( executor.binDir )
            result.add(executor.binDir)
        return result
    }

    @Memoized
    boolean isLocalWorkDir() {
        return executor.workDir.fileSystem == FileSystems.default
    }

    /**
     * @return The map holding the shell environment variables for the task to be executed
     */
    @Memoized
    Map<String,String> getProcessEnvironment() {

        def result = new LinkedHashMap<String,String>(20)

        // add the taskConfig environment entries
        if( session.config.env instanceof Map ) {
            session.config.env.each { name, value ->
                result.put( name, value?.toString() )
            }
        }
        else {
            log.debug "Invalid 'session.config.env' object: ${session.config.env?.class?.name}"
        }

        // append the 'bin' folder to the task environment
        List<Path> paths
        if( isLocalWorkDir() && (paths=getBinDirs()) ) {
            for( Path it : paths ) {
                if( result.containsKey('PATH') ) {
                    // note: do not escape potential blanks in the bin path because the PATH
                    // variable is enclosed in `"` when in rendered in the launcher script -- see #630
                    result['PATH'] =  "${result['PATH']}:${it}".toString()
                }
                else {
                    // note: append custom bin path *after* the system PATH
                    // to prevent unnecessary network round-trip for each command
                    // when the added path is a shared file system directory
                    result['PATH'] = "\$PATH:${it}".toString()
                }
            }
        }

        return Collections.unmodifiableMap(result)
    }

    protected Path resolvePath(Object item) {
        final result = normalizeToPath(item)
        return result instanceof LogicalDataPath
            ? result.toTargetPath()
            : result
    }

    /**
     * An input file parameter can be provided with any value other than a file.
     * This function normalize a generic value to a {@code Path} create a temporary file
     * in the for it.
     *
     * @param input The input value
     * @param altName The name to be used when a temporary file is created.
     * @return The {@code Path} that will be staged in the task working folder
     */
    protected FileHolder normalizeInputToFile( Object input, String altName ) {
        /*
         * when it is a local file, just return a reference holder to it
         */
        if( input instanceof Path ) {
            return new FileHolder(input)
        }

        /*
         * default case, convert the input object to a string and save
         * to a local file
         */
        def source = input?.toString() ?: ''
        def result = Nextflow.tempFile(altName)
        result.text = source
        return new FileHolder(source, result)
    }

    protected Path normalizeToPath( obj ) {
        if( obj instanceof Path )
            return obj

        if( obj == null )
            throw new ProcessUnrecoverableException("Path value cannot be null")
        
        if( !(obj instanceof CharSequence) )
            throw new ProcessUnrecoverableException("Not a valid path value type: ${obj.getClass().getName()} ($obj)")

        def str = obj.toString().trim()
        if( str.contains('\n') )
            throw new ProcessUnrecoverableException("Path value cannot contain a new-line character: $str")
        if( str.startsWith('/') )
            return FileHelper.asPath(str)
        if( FileHelper.getUrlProtocol(str) )
            return FileHelper.asPath(str)
        if( !str )
            throw new ProcessUnrecoverableException("Path value cannot be empty")
        
        throw new ProcessUnrecoverableException("Not a valid path value: '$str'")
    }

    protected List<FileHolder> normalizeInputToFiles( Object obj, int count, boolean coerceToPath, FilePorter.Batch foreignFiles ) {

        Collection allItems = obj instanceof Collection ? obj : [obj]
        def len = allItems.size()

        // use a bag so that cache hash key is not affected by file entries order
        def files = new ArrayBag<FileHolder>(len)
        for( def item : allItems ) {

            if( item instanceof Path || coerceToPath ) {
                final path = resolvePath(item)
                final target = executor.isForeignFile(path) ? foreignFiles.addToForeign(path) : path
                final holder = new FileHolder(target)
                files << holder
            }
            else {
                files << normalizeInputToFile(item, "input.${++count}")
            }
        }

        return files
    }

    protected singleItemOrList( List<FileHolder> items, boolean single, ScriptType type ) {
        assert items != null

        if( items.size() == 1 && single ) {
            return makePath(items[0],type)
        }

        def result = new ArrayList(items.size())
        for( int i=0; i<items.size(); i++ ) {
            result.add( makePath(items[i],type) )
        }
        return new BlankSeparatedList(result)
    }

    private Path makePath( FileHolder holder, ScriptType type ) {
        if( type == ScriptType.SCRIPTLET ) {
            return new TaskPath(holder)
        }
        if( type == ScriptType.GROOVY) {
            // the real path for the native task needs to be fixed -- see #378
            return Paths.get(holder.stageName)
        }
        throw new IllegalStateException("Unknown task type: $type")
    }


    /**
     * An input file name may contain wildcards characters which have to be handled coherently
     * given the number of files specified.
     *
     * @param name A file name with may contain a wildcard character star {@code *} or question mark {@code ?}.
     *  Only one occurrence can be specified for star or question mark wildcards.
     *
     * @param value Any value that have to be managed as an input files. Values other than {@code Path} are converted
     * to a string value, using the {@code #toString} method and saved in the local file-system. Value of type {@code Collection}
     * are expanded to multiple values accordingly.
     *
     * @return
     */
    @CompileStatic
    protected List<FileHolder> expandWildcards( String name, List<FileHolder> files ) {
        assert files != null

        // use an unordered so that cache hash key is not affected by file entries order
        final result = new ArrayBag(files.size())
        if( files.size()==0 ) { return result }

        if( !name || name == '*' ) {
            result.addAll(files)
            return result
        }

        if( !name.contains('*') && !name.contains('?') && files.size()>1 ) {
            /*
             * When name do not contain any wildcards *BUT* multiple files are provide
             * it is managed like having a 'star' at the end of the file name
             */
            name += '*'
        }

        for( int i=0; i<files.size(); i++ ) {
            def holder = files[i]
            def newName = expandWildcards0(name, holder.stageName, i+1, files.size())
            result << holder.withName( newName )
        }

        return result
    }

    @CompileStatic
    protected String replaceQuestionMarkWildcards(String name, int index) {
        def result = new StringBuffer()

        Matcher m = QUESTION_MARK.matcher(name)
        while( m.find() ) {
            def match = m.group(1)
            def repString = String.valueOf(index).padLeft(match.size(), '0')
            m.appendReplacement(result, repString)
        }
        m.appendTail(result)
        result.toString()
    }

    @CompileStatic
    protected String replaceStarWildcards(String name, int index, boolean strip=false) {
        name.replaceAll(/\*/, strip ? '' : String.valueOf(index))
    }

    @CompileStatic
    protected String expandWildcards0( String path, String stageName, int index, int size ) {

        String name
        String parent
        int p = path.lastIndexOf('/')
        if( p == -1 ) {
            parent = null
            name = path
        }
        else {
            parent = path.substring(0,p)
            name = path.substring(p+1)
        }

        if( name == '*' || !name ) {
            name = stageName
        }
        else {
            final stripWildcard = size<=1 // <-- string the start wildcard instead of expanding to an index number when the collection contain only one file
            name = replaceStarWildcards(name, index, stripWildcard)
            name = replaceQuestionMarkWildcards(name, index)
        }

        if( parent ) {
            parent = replaceStarWildcards(parent, index)
            parent = replaceQuestionMarkWildcards(parent, index)
            return "$parent/$name"
        }
        else {
            return name
        }

    }

    /**
     * Given a map holding variables key-value pairs, create a script fragment
     * exporting the required environment variables
     */
    static String bashEnvironmentScript( Map<String,String> environment, boolean escape=false ) {
        if( !environment )
            return null

        final List script = []
        for( String name : environment.keySet() ) {
            String value = environment.get(name)
            if( !ENV_VAR_NAME.matcher(name).matches() )
                log.trace "Illegal environment variable name: '${name}' -- This variable definition is ignored"
            else if( !value ) {
                log.warn "Environment variable `$name` evaluates to an empty value"
                script << "export $name=''"
            }
            else if( !escape ) {
                script << /export $name="$value"/
            }
            else {
                // escape both wrapping double quotes and the dollar var placeholder
                script << /export $name="${Escape.variable(value)}"/
            }
        }
        script << ''

        return script.join('\n')
    }

    final protected int makeTaskContextStage1( TaskRun task, Map secondPass, List values ) {

        final contextMap = task.context
        int count = 0

        task.inputs.keySet().each { InParam param ->

            // add the value to the task instance
            def val = param.decodeInputs(values)

            switch(param) {
                case ValueInParam:
                    contextMap.put( param.name, val )
                    break

                case FileInParam:
                    secondPass[param] = val
                    return // <-- leave it, because we do not want to add this 'val' at this stage

                case StdInParam:
                case EnvInParam:
                    // nothing to do
                    break

                default:
                    throw new IllegalStateException("Unsupported input param type: ${param?.class?.simpleName}")
            }

            // add the value to the task instance context
            task.setInput(param, val)
        }

        return count
    }

    final protected FilePorter.Batch makeTaskContextStage2( TaskRun task, Map secondPass, int count ) {

        final ctx = task.context
        final allNames = new HashMap<String,Integer>()

        final FilePorter.Batch foreignFiles = session.filePorter.newBatch(executor.getStageDir())

        // -- all file parameters are processed in a second pass
        //    so that we can use resolve the variables that eventually are in the file name
        for( Map.Entry<FileInParam,?> entry : secondPass.entrySet() ) {
            final param = entry.getKey()
            final val = entry.getValue()
            final fileParam = param as FileInParam
            final normalized = normalizeInputToFiles(val, count, fileParam.isPathQualifier(), foreignFiles)
            final resolved = expandWildcards( fileParam.getFilePattern(ctx), normalized )

            if( !param.isValidArity(resolved.size()) )
                throw new IllegalArityException("Incorrect number of input files for process `${safeTaskName(task)}` -- expected ${param.arity}, found ${resolved.size()}")

            ctx.put( param.name, singleItemOrList(resolved, param.isSingle(), task.type) )
            count += resolved.size()
            for( FileHolder item : resolved ) {
                Integer num = allNames.getOrCreate(item.stageName, 0) +1
                allNames.put(item.stageName,num)
            }

            // add the value to the task instance context
            task.setInput(param, resolved)
        }

        // -- set the delegate map as context in the task config
        //    so that lazy directives will be resolved against it
        task.config.context = ctx

        // check conflicting file names
        def conflicts = allNames.findAll { name, num -> num>1 }
        if( conflicts ) {
            log.debug("Process $name > collision check staging file names: $allNames")
            def message = "Process `$name` input file name collision -- There are multiple input files for each of the following file names: ${conflicts.keySet().join(', ')}"
            throw new ProcessUnrecoverableException(message)
        }
        return foreignFiles
    }

    protected void makeTaskContextStage3( TaskRun task, HashCode hash, Path folder ) {
        // set hash-code & working directory
        task.hash = hash
        task.workDir = folder
        task.config.workDir = folder
        task.config.hash = hash.toString()
        task.config.name = task.getName()
    }

    final protected HashCode createTaskHashKey(TaskRun task) {

        List keys = [ session.uniqueId, name, task.source ]

        if( task.isContainerEnabled() )
            keys << task.getContainerFingerprint()

        // add all the input name-value pairs to the key generator
        for( Map.Entry<InParam,Object> it : task.inputs ) {
            keys.add( it.key.name )
            keys.add( it.value )
        }

        // add all variable references in the task script but not declared as input/output
        def vars = getTaskGlobalVars(task)
        if( vars ) {
            log.trace "Task: $name > Adding script vars hash code: ${vars}"
            keys.add(vars.entrySet())
        }

        final binEntries = getTaskBinEntries(task.source)
        if( binEntries ) {
            log.trace "Task: $name > Adding scripts on project bin path: ${-> binEntries.join('; ')}"
            keys.addAll(binEntries)
        }

        final modules = task.getConfig().getModule()
        if( modules ) {
            keys.addAll(modules)
        }
        
        final conda = task.getCondaEnv()
        if( conda ) {
            keys.add(conda)
        }

        final spack = task.getSpackEnv()
        final arch = task.getConfig().getArchitecture()

        if( spack ) {
            keys.add(spack)

            if( arch ) {
                keys.add(arch)
            }
        }

        if( session.stubRun && task.config.getStubBlock() ) {
            keys.add('stub-run')
        }

        final mode = config.getHashMode()
        final hash = computeHash(keys, mode)
        if( session.dumpHashes ) {
            session.dumpHashes=='json'
                ? traceInputsHashesJson(task, keys, mode, hash)
                : traceInputsHashes(task, keys, mode, hash)
        }
        return hash
    }

    HashCode computeHash(List keys, CacheHelper.HashMode mode) {
        try {
            return CacheHelper.hasher(keys, mode).hash()
        }
        catch (Throwable e) {
            final msg = "Something went wrong while creating task '$name' unique id -- Offending keys: ${ keys.collect {"\n - type=${it.getClass().getName()} value=$it"} }"
            throw new UnexpectedException(msg,e)
        }
    }


    /**
     * This method scans the task command string looking for invocations of scripts
     * defined in the project bin folder.
     *
     * @param script The task command string
     * @return The list of paths of scripts in the project bin folder referenced in the task command
     */
    @Memoized
    List<Path> getTaskBinEntries(String script) {
        List<Path> result = []
        def tokenizer = new StringTokenizer(script," \t\n\r\f()[]{};&|<>`")
        while( tokenizer.hasMoreTokens() ) {
            def token = tokenizer.nextToken()
            def path = session.binEntries.get(token)
            if( path )
                result.add(path)
        }
        return result
    }

    private void traceInputsHashesJson( TaskRun task, List entries, CacheHelper.HashMode mode, hash ) {
        final collector = (item) -> [
            hash: CacheHelper.hasher(item, mode).hash().toString(),
            type: item?.getClass()?.getName(),
            value: item?.toString()
        ]
        final json = JsonOutput.toJson(entries.collect(collector))
        log.info "[${safeTaskName(task)}] cache hash: ${hash}; mode: ${mode}; entries: ${JsonOutput.prettyPrint(json)}"
    }

    private void traceInputsHashes( TaskRun task, List entries, CacheHelper.HashMode mode, hash ) {

        def buffer = new StringBuilder()
        buffer.append("[${safeTaskName(task)}] cache hash: ${hash}; mode: $mode; entries: \n")
        for( Object item : entries ) {
            buffer.append( "  ${CacheHelper.hasher(item, mode).hash()} [${item?.getClass()?.getName()}] $item \n")
        }

        log.info(buffer.toString())
    }

    Map<String,Object> getTaskGlobalVars(TaskRun task) {
        final result = task.getGlobalVars(ownerScript.binding)
        final directives = getTaskExtensionDirectiveVars(task)
        result.putAll(directives)
        return result
    }

    protected Map<String,Object> getTaskExtensionDirectiveVars(TaskRun task) {
        final variableNames = task.getVariableNames()
        final result = new HashMap(variableNames.size())
        final taskConfig = task.config
        for( String key : variableNames ) {
            if( !key.startsWith('task.ext.') ) continue
            final value = taskConfig.eval(key.substring(5))
            result.put(key, value)
        }

        return result
    }

    /**
     * Execute the specified task shell script
     *
     * @param script The script string to be execute, e.g. a BASH script
     * @return {@code TaskDef}
     */
    final protected void submitTask( TaskRun task, HashCode hash, Path folder ) {
        log.trace "[${safeTaskName(task)}] actual run folder: ${folder}"

        makeTaskContextStage3(task, hash, folder)

        // when no collector is define OR it's a task retry, then submit directly for execution
        if( !arrayCollector || task.config.getAttempt() > 1 )
            executor.submit(task)
        // add the task to the collection of running tasks
        else
            arrayCollector.collect(task)
    }

    protected boolean checkWhenGuard(TaskRun task) {

        try {
            def pass = task.config.getWhenGuard()
            if( pass ) {
                return true
            }

            log.trace "Task ${safeTaskName(task)} is not executed because `when` condition is not verified"
            finalizeTask0(task)
            return false
        }
        catch ( FailedGuardException error ) {
            handleException(error, task)
            return false
        }
    }

    /**
     * Finalize the task execution, checking the exit status
     * and binding output values accordingly
     *
     * @param task The {@code TaskRun} instance to finalize
     */
    @PackageScope
    final finalizeTask( TaskHandler handler) {
        def task = handler.task
        log.trace "finalizing process > ${safeTaskName(task)} -- $task"

        def fault = null
        try {
            // -- verify task exit status
            if( task.error )
                throw new ProcessFailedException("Process `${safeTaskName(task)}` failed", task.error)

            if( task.type == ScriptType.SCRIPTLET ) {
                if( task.exitStatus == Integer.MAX_VALUE )
                    throw new ProcessFailedException("Process `${safeTaskName(task)}` terminated for an unknown reason -- Likely it has been terminated by the external system")

                if ( !task.isSuccess() )
                    throw new ProcessFailedException("Process `${safeTaskName(task)}` terminated with an error exit status (${task.exitStatus})")
            }

            // -- expose task exit status to make accessible as output value
            task.config.exitStatus = task.exitStatus
            // -- if it's OK collect results and finalize
            collectOutputs(task)
        }
        catch ( Throwable error ) {
            fault = resumeOrDie(task, error, handler.getTraceRecord())
            log.trace "Task fault (3): $fault"
        }

        // -- finalize the task
        if( fault != ErrorStrategy.RETRY )
            finalizeTask0(task)

        return fault
    }

    /**
     * Whenever the process can be cached
     */
    boolean isCacheable() {
        session.cacheable && config.cacheable
    }

    @PackageScope boolean isResumable() {
        isCacheable() && session.resumeMode
    }

    /**
     * Finalize the task execution, checking the exit status
     * and binding output values accordingly
     *
     * @param task The {@code TaskRun} instance to finalize
     * @param producedFiles The map of files to be bind the outputs
     */
    private void finalizeTask0( TaskRun task ) {
        log.trace "Finalize process > ${safeTaskName(task)}"

        // -- bind output (files)
        if( task.canBind ) {
            bindOutputs(task)
            publishOutputs(task)
        }

        // increment the number of processes executed
        state.update { StateObj it -> it.incCompleted() }
    }

    protected void closeProcess() {
        arrayCollector?.close()
    }

    protected void terminateProcess() {
        log.trace "<${name}> Sending poison pills and terminating process"
        sendPoisonPill()
        session.notifyProcessTerminate(this)
        session.processDeregister(this)
    }

    /**
     * Dump the current process status listing
     * all input *port* statuses for debugging purpose
     *
     * @return The text description representing the process status
     */
    String dumpTerminationStatus() {

        def result = new StringBuilder()
        def terminated = !DataflowHelper.isProcessorActive(operator)
        result << "[process] $name\n"
        if( terminated )
            return result.toString()

        def statusStr = !completed && !terminated ? 'status=ACTIVE' : ( completed && terminated ? 'status=TERMINATED' : "completed=$completed; terminated=$terminated" )
        result << "  $statusStr\n"
        // add extra info about port statuses
        for( int i=0; i<openPorts.length(); i++ ) {
            def last = i == openPorts.length()-1
            def param = config.getInputs()[i]
            def chnnl = param?.inChannel
            def isValue = chnnl instanceof DataflowExpression
            def type = last ? '(cntrl)' : (isValue ? '(value)' : '(queue)')
            def channel = param && !(param instanceof TupleInParam) ? param.getName() : '-'
            def status; if( isValue ) { status = !chnnl.isBound() ? 'OPEN  ' : 'bound ' }
            else status = type == '(queue)' ? (openPorts.get(i) ? 'OPEN  ' : 'closed') : '-     '
            result << "  port $i: $type ${status}; channel: $channel\n"
        }

        return result.toString()
    }

    /*
     * logger class for the *iterator* processor
     */
    class BaseProcessInterceptor extends DataflowEventAdapter {

        final List<DataflowReadChannel> inputs

        final boolean stopAfterFirstRun

        final int len

        final DataflowQueue control

        final int first

        BaseProcessInterceptor( List<DataflowReadChannel> inputs, boolean stop ) {
            this.inputs = new ArrayList<>(inputs)
            this.stopAfterFirstRun = stop
            this.len = inputs.size()
            this.control = (DataflowQueue)inputs.get(len-1)
            this.first = inputs.findIndexOf { CH.isChannelQueue(it) }
        }

        @Override
        Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            if( len == 1 || stopAfterFirstRun ) {
                // -- kill itself
                control.bind(PoisonPill.instance)
            }
            else if( index == first ) {
                // the `if` condition guarantees only and only one signal message (the true value)
                // is bound to the control message for a complete set of input values delivered
                // to the process -- the control message is need to keep the process running
                control.bind(Boolean.TRUE)
            }

            return message
        }
    }

    /**
     *  Intercept dataflow process events
     */
    class TaskProcessorInterceptor extends BaseProcessInterceptor {

        TaskProcessorInterceptor(List<DataflowReadChannel> inputs, boolean stop) {
            super(inputs, stop)
        }

        @Override
        List<Object> beforeRun(final DataflowProcessor processor, final List<Object> messages) {
            // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
            if( log.isTraceEnabled() )
                log.trace "<${name}> Before run -- messages: ${messages}"
            // the counter must be incremented here, otherwise it won't be consistent
            state.update { StateObj it -> it.incSubmitted() }
            // task index must be created here to guarantee consistent ordering
            // with the sequence of messages arrival since this method is executed in a thread safe manner
            final params = new TaskStartParams(TaskId.next(), indexCount.incrementAndGet())
            final result = new ArrayList(2)
            result[0] = params
            result[1] = messages
            return result
        }


        @Override
        void afterRun(DataflowProcessor processor, List<Object> messages) {
            // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
            if( log.isTraceEnabled() )
                log.trace "<${name}> After run"
            currentTask.remove()
        }

        @Override
        Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
            if( log.isTraceEnabled() ) {
                def channelName = config.getInputs()?.names?.get(index)
                def taskName = currentTask.get()?.name ?: name
                log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
            }

            super.messageArrived(processor, channel, index, message)
        }

        @Override
        Object controlMessageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
            if( log.isTraceEnabled() ) {
                def channelName = config.getInputs()?.names?.get(index)
                def taskName = currentTask.get()?.name ?: name
                log.trace "<${taskName}> Control message arrived ${channelName} => ${message}"
            }

            super.controlMessageArrived(processor, channel, index, message)

            if( message == PoisonPill.instance ) {
                // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
                if( log.isTraceEnabled() )
                    log.trace "<${name}> Poison pill arrived; port: $index"
                openPorts.set(index, 0) // mark the port as closed
                state.update { StateObj it -> it.poison() }
            }

            return message
        }

        @Override
        void afterStop(final DataflowProcessor processor) {
            // apparently auto if-guard instrumented by @Slf4j is not honoured in inner classes - add it explicitly
            if( log.isTraceEnabled() )
                log.trace "<${name}> After stop"
            closeProcess()
        }

        /**
         * Invoked if an exception occurs. Unless overridden by subclasses this implementation returns true to terminate the operator.
         * If any of the listeners returns true, the operator will terminate.
         * Exceptions outside of the operator's body or listeners' messageSentOut() handlers will terminate the operator irrespective of the listeners' votes.
         * When using maxForks, the method may be invoked from threads running the forks.
         * @param processor
         * @param error
         * @return
         */
        boolean onException(final DataflowProcessor processor, final Throwable error) {
            // return `true` to terminate the dataflow processor
            handleException( error, currentTask.get() )
        }
    }
}
