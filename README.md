# poisson-ticker

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
