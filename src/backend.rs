use std::ffi::CString;

use anyhow::{Context, Result};
use hidapi::HidApi;

use crate::layout::DeviceDescriptor;

pub trait Backend: Send + Sync + 'static {
    fn enumerate(&self) -> Result<Vec<DeviceDescriptor>>;
    fn open(&self, path: &[u8]) -> Result<Box<dyn DeviceHandle>>;
}

pub trait DeviceHandle {
    fn get_input_report(&mut self, report_id: u8, read_size: usize) -> Result<Vec<u8>>;
    fn read(&mut self, read_size: usize, timeout_ms: i32) -> Result<Vec<u8>>;
    fn write(&mut self, payload: &[u8]) -> Result<usize>;
}

#[derive(Debug, Default)]
pub struct HidBackend;

impl Backend for HidBackend {
    fn enumerate(&self) -> Result<Vec<DeviceDescriptor>> {
        let api = HidApi::new().context("creating hidapi context")?;
        let mut devices = Vec::new();
        for info in api.device_list() {
            let serial_number = info
                .serial_number()
                .map(ToOwned::to_owned)
                .unwrap_or_default();
            devices.push(DeviceDescriptor {
                path: info.path().to_bytes().to_vec(),
                vendor_id: info.vendor_id(),
                product_id: info.product_id(),
                serial_number,
                usage_page: usage_page(info),
                interface_number: Some(info.interface_number()),
            });
        }
        Ok(devices)
    }

    fn open(&self, path: &[u8]) -> Result<Box<dyn DeviceHandle>> {
        let api = HidApi::new().context("creating hidapi context")?;
        let path = CString::new(path.to_vec()).context("path contains interior nul")?;
        let device = api.open_path(&path).context("opening hid device")?;
        Ok(Box::new(HidDeviceHandle { device }))
    }
}

struct HidDeviceHandle {
    device: hidapi::HidDevice,
}

impl DeviceHandle for HidDeviceHandle {
    fn get_input_report(&mut self, report_id: u8, read_size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; read_size];
        buffer[0] = report_id;
        let size = self
            .device
            .get_input_report(&mut buffer)
            .context("reading input report")?;
        buffer.truncate(size);
        Ok(buffer)
    }

    fn read(&mut self, read_size: usize, timeout_ms: i32) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; read_size];
        let size = self
            .device
            .read_timeout(&mut buffer, timeout_ms)
            .context("reading hid report")?;
        buffer.truncate(size);
        Ok(buffer)
    }

    fn write(&mut self, payload: &[u8]) -> Result<usize> {
        Ok(self.device.write(payload).context("writing hid report")?)
    }
}

fn usage_page(info: &hidapi::DeviceInfo) -> Option<u16> {
    #[cfg(target_os = "linux")]
    {
        let _ = info;
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        if info.usage_page() == 0 {
            None
        } else {
            Some(info.usage_page())
        }
    }
}
