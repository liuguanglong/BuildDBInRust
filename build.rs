


fn main() -> Result<(), Box<dyn std::error::Error>> {

    tonic_build::configure()
    .build_server(true)
    .build_client(false)
    .out_dir("src/server/")
    .compile(&["proto/db.proto"], &["proto/"])?;

    tonic_build::configure()
    .build_client(true)
    .build_server(false)
    .out_dir("src/client/")
    .compile(&["proto/db.proto"], &["proto/"])?;

    Ok(())

}