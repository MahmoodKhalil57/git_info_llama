fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        println!("Hello, {}!", args[1]);
    } else {
        println!("Hello, World!");
    }
}