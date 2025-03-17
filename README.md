[![progress-banner](https://backend.codecrafters.io/progress/kafka/1447b215-8f08-4543-aa4f-1c565c2bdfe8)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to APIVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.


# Refactoring 
### Newtype macro

### Enums

Enum Variants as types are not supported in rust 
this is a common workaround 

```rust
struct Dog {}
struct Whale {}
enum Animal {
    Dog(Dog),
    Whale(Whale),
}
```

