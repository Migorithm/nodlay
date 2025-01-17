use playground::vector_clock::vector_clock_test;

#[tokio::main]
async fn main() {
    vector_clock_test().await;
}
