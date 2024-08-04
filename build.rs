


fn main() -> Result<(), Box<dyn std::error::Error>> {

    tonic_build::configure()
    .build_server(true)
    .out_dir("src/server/")
    .compile(&["proto/db.proto"], &["proto/"])?;

    tonic_build::configure()
    .build_client(true)
    .out_dir("src/client/")
    .compile(&["proto/db.proto"], &["proto/"])?;

    Ok(())

}