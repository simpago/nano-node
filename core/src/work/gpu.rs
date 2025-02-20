use ocl::{
    self,
    builders::{DeviceSpecifier, ProgramBuilder},
    flags::MemFlags,
    Buffer, Platform, ProQue, Result,
};
use tracing::info;

pub struct Gpu {
    kernel: ocl::Kernel,
    attempt: Buffer<u8>,
    result: Buffer<u8>,
    root: Buffer<u8>,
}

impl Gpu {
    pub fn new(
        platform_idx: usize,
        device_idx: usize,
        threads: usize,
        local_work_size: Option<usize>,
    ) -> Result<Gpu> {
        let mut prog_bldr = ProgramBuilder::new();
        prog_bldr.src(include_str!("work.cl"));
        let platforms = Platform::list();
        if platforms.len() == 0 {
            return Err("No OpenCL platforms exist (check your drivers and OpenCL setup)".into());
        }
        if platform_idx >= platforms.len() {
            return Err(format!(
                "Platform index {} too large (max {})",
                platform_idx,
                platforms.len() - 1
            )
            .into());
        }
        let pro_que = ProQue::builder()
            .prog_bldr(prog_bldr)
            .platform(platforms[platform_idx])
            .device(DeviceSpecifier::Indices(vec![device_idx]))
            .dims(1)
            .build()?;

        let device = pro_que.device();
        info!(
            "Initializing GPU: {} {}",
            device.vendor().unwrap_or_else(|_| "[unknown]".into()),
            device.name().unwrap_or_else(|_| "[unknown]".into())
        );

        let attempt = Buffer::<u8>::builder()
            .queue(pro_que.queue().clone())
            .flags(MemFlags::new().read_only().host_write_only())
            .len(8)
            .build()?;
        let result = Buffer::<u8>::builder()
            .queue(pro_que.queue().clone())
            .flags(MemFlags::new().write_only())
            .len(8)
            .build()?;
        let root = Buffer::<u8>::builder()
            .queue(pro_que.queue().clone())
            .flags(MemFlags::new().read_only().host_write_only())
            .len(32)
            .build()?;

        let difficulty = 0u64;

        let kernel = {
            let mut kernel_builder = pro_que.kernel_builder("nano_work");
            kernel_builder
                .global_work_size(threads)
                .arg(&attempt)
                .arg(&result)
                .arg(&root)
                .arg_named("difficulty", &difficulty);
            if let Some(local_work_size) = local_work_size {
                kernel_builder.local_work_size(local_work_size);
            }
            kernel_builder.build()?
        };

        let mut gpu = Gpu {
            kernel,
            attempt,
            result,
            root,
        };
        gpu.reset_bufs()?;
        Ok(gpu)
    }

    pub fn reset_bufs(&mut self) -> Result<()> {
        self.result.write(&[0u8; 8] as &[u8]).enq()?;
        Ok(())
    }

    pub fn set_task(&mut self, root: &[u8], difficulty: u64) -> Result<()> {
        self.reset_bufs()?;
        self.root.write(root).enq()?;
        self.kernel.set_arg("difficulty", difficulty)?;
        Ok(())
    }

    pub fn run(&mut self, out: &mut [u8], attempt: u64) -> Result<bool> {
        let attempt_bytes = attempt.to_le_bytes();
        self.attempt.write(&attempt_bytes as &[u8]).enq()?;
        debug_assert!(out.iter().all(|&b| b == 0));
        debug_assert!({
            let mut result = [0u8; 8];
            self.result.read(&mut result as &mut [u8]).enq()?;
            result.iter().all(|&b| b == 0)
        });

        unsafe {
            self.kernel.enq()?;
        }

        self.result.read(&mut *out).enq()?;
        let success = !out.iter().all(|&b| b == 0);
        if success {
            self.reset_bufs()?;
        }
        Ok(success)
    }
}
