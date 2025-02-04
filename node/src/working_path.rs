use rsnano_core::Networks;
use std::path::PathBuf;
use uuid::Uuid;

pub fn working_path_for(network: Networks) -> Option<PathBuf> {
    if let Ok(path_override) = std::env::var("NANO_APP_PATH") {
        eprintln!(
            "Application path overridden by NANO_APP_PATH environment variable: {path_override}"
        );
        return Some(path_override.into());
    }

    dirs::home_dir().and_then(|mut path| {
        let subdir = match network {
            Networks::Invalid => return None,
            Networks::NanoDevNetwork => "NanoDev",
            Networks::NanoBetaNetwork => "NanoBeta",
            Networks::NanoLiveNetwork => "Nano",
            Networks::NanoTestNetwork => "NanoTest",
        };
        path.push(subdir);
        Some(path)
    })
}

pub fn unique_path() -> Option<PathBuf> {
    unique_path_for(Networks::NanoDevNetwork)
}

fn unique_path_for(network: Networks) -> Option<PathBuf> {
    working_path_for(network).map(|mut path| {
        let uuid = Uuid::new_v4();
        path.push(uuid.to_string());
        std::fs::create_dir_all(&path).unwrap();
        path
    })
}
