# Ictarus
An (unofficial) IOTA Ict node implementation written in Rust.

## Disclaimer
!!! THIS IS WIP. DO NOT USE THIS CODE FOR OTHER THINGS THAN READING SOME RUST CODE OR INTENDING TO HELP MAKING IT BETTER!!!

Also: Don't expect feature parity with the Java Ict core anytime soon. This is just me having fun with Rust trying to facilitate this great language to do things a little bit more efficiently than the [reference implementation](https://github.com/iotaledger/ict.git) which is written in Java and intended for fast prototyping. If anything useful comes out of it, I'm more than happy, if not, then I still have learned a great deal of Rust, and maybe someone else as well. Contributions and discussions how things can be done better/more efficiently than I am currently doing are highly appreciated. I am still far off of being a Rust expert. Contact me on the IOTA Discord server (/alex/#6323).

## Current Focus:
Finding a more performant implementation for the Tangle struct, that allows for updating edges in an efficient, unordered and asynchronous manner. Some unit tests and all integration tests (deactivated) are currently failing due to this. If you want to run the tests, better use `cargo test --release`, otherwise they will take too long.

