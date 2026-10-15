#[cfg(not(target_os = "linux"))]
compile_error!(
    "bkxpro is a Linux-only microservice utilizing OS-level facilities (io_uring, O_DIRECT). \
     Non-Linux compilation is strictly unsupported."
);
