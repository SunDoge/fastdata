use libvips::{
    ops::{self, ResizeOptions},
    VipsApp, VipsImage,
};

fn main() {
    // this initializes the libvips library. it has to live as long as the application lives (or as long as you want to use the library within your app)
    // you can't have multiple objects of this type and when it is dropped it will call the libvips functions to free all internal structures.
    let app = VipsApp::new("Test Libvips", false).expect("Cannot initialize libvips");
    //set number of threads in libvips's threadpool
    app.concurrency_set(2);
    // loads an image from file
    let image = VipsImage::new_from_file("beforescale.jpg").unwrap();

    // will resized the image and return a new instance.
    // libvips works most of the time with immutable objects, so it will return a new object
    // the VipsImage struct implements Drop, which will free the memory
    let resized = ops::resize_with_opts(
        &image,
        0.66666,
        &ResizeOptions {
            kernel: ops::Kernel::Linear,
            vscale: 0.66666,
            ..Default::default()
        },
    )
    .unwrap();

    //optional parameters
    // let options = ops::JpegsaveOptions {
    //     q: 90,
    //     background: vec![255.0],
    //     strip: true,
    //     optimize_coding: true,
    //     optimize_scans: true,
    //     interlace: true,
    //     ..ops::JpegsaveOptions::default()
    // };

    // // alternatively you can use `jpegsave` that will use the default options
    // match ops::jpegsave_with_opts(&resized, "output.jpeg",  &options) {
    //     Err(_) => println!("error: {}", app.error_buffer().unwrap()),
    //     Ok(_) => println!("Great Success!")
    // }
    libvips::ops::jpegsave(&resized, "output.jpg").unwrap();
}
