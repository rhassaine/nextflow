channel.of( 1, 1, 2, 2, 2, 3, 1, 1, 2, 4, 6 )
    .unique { v -> v % 2 }
    .view()