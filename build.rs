fn main() {
    // Embed fonts and icons into the binary: the file is scp'ed alone to the Pi.
    let config = slint_build::CompilerConfiguration::new()
        .embed_resources(slint_build::EmbedResourcesKind::EmbedFiles);
    slint_build::compile_with_config("ui/app.slint", config).expect("Slint build failed");
}
