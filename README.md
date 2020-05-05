# poisson-ticker

[![Crates.io](https://img.shields.io/crates/v/poisson-ticker.svg)](https://crates.io/crates/poisson-ticker)
[![Documentation](https://docs.rs/poisson-ticker)](https://docs.rs/poisson-ticker)
![Build Status](https://github.com/akshayknarayan/poisson-ticker/workflows/Rust/badge.svg)

Exponentially distributed timer for your Poisson-arrivals needs.

## Example
```rust
#[tokio::main]
async fn main() {
    let mut t = poisson_ticker::Ticker::new(std::time::Duration::from_millis(10));
    let now = std::time::Instant::now();
    for _ in 0usize..5 {
        (&mut t).await;
    }
    println!("elapsed: {:?}", now.elapsed());
}
```

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
