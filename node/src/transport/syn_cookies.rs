use std::{
    collections::HashMap,
    net::{Ipv6Addr, SocketAddrV6},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Result;
use rand::Rng;
use rsnano_core::{utils::ContainerInfo, Account, Signature};
use rsnano_messages::Cookie;

/// Node ID cookies for node ID handshakes
pub struct SynCookies {
    data: Mutex<LockedSynCookies>,
    max_cookies_per_ip: usize,
}

impl SynCookies {
    pub fn new(max_cookies_per_ip: usize) -> Self {
        Self {
            data: Mutex::new(LockedSynCookies {
                cookies: HashMap::new(),
                cookies_per_ip: HashMap::new(),
            }),
            max_cookies_per_ip,
        }
    }

    /// Returns `None` if the IP is rate capped on syn cookie requests,
    /// or if the endpoint already has a syn cookie query
    pub fn assign(&self, endpoint: &SocketAddrV6) -> Option<Cookie> {
        let ip_addr = endpoint.ip();
        let mut lock = self.data.lock().unwrap();

        if lock.cookies.contains_key(endpoint) {
            return None;
        }

        let ip_cookies = lock.cookies_per_ip.entry(*ip_addr).or_default();
        if *ip_cookies < self.max_cookies_per_ip {
            *ip_cookies += 1;
            let cookie = rand::rng().random::<Cookie>();
            lock.cookies.insert(
                *endpoint,
                SynCookieInfo {
                    cookie,
                    created_at: Instant::now(),
                },
            );
            Some(cookie)
        } else {
            None
        }
    }

    // Returns `false` if invalid, `true` if valid
    // Also removes the syn cookie from the store if valid
    pub fn validate(
        &self,
        endpoint: &SocketAddrV6,
        node_id: &Account,
        signature: &Signature,
    ) -> Result<()> {
        let ip_addr = endpoint.ip();
        let mut lock = self.data.lock().unwrap();
        if let Some(info) = lock.cookies.get(endpoint) {
            node_id.as_key().verify(&info.cookie, signature)?;
            lock.cookies.remove(endpoint);
            lock.dec_cookie_count(*ip_addr);
        }
        Ok(())
    }

    pub fn purge(&self, cutoff: Duration) {
        let mut lock = self.data.lock().unwrap();
        let now = Instant::now();
        //todo use drain_filter once it is stabelized
        let mut removed_endpoints = Vec::new();
        for (endpoint, _info) in lock
            .cookies
            .iter()
            .filter(|(_k, v)| v.exceeds_cutoff(cutoff, now))
        {
            removed_endpoints.push(*endpoint);
        }

        for endpoint in &removed_endpoints {
            lock.cookies.remove(endpoint);
            lock.dec_cookie_count(*endpoint.ip());
        }
    }

    /// Get cookie associated with endpoint and erases that cookie from this container
    pub fn cookie(&self, endpoint: &SocketAddrV6) -> Option<Cookie> {
        let ip_addr = endpoint.ip();
        let mut lock = self.data.lock().unwrap();
        let info = lock.cookies.remove(endpoint);
        if info.is_some() {
            lock.dec_cookie_count(*ip_addr);
        }
        info.map(|i| i.cookie)
    }

    pub fn cookies_count(&self) -> usize {
        self.data.lock().unwrap().cookies.len()
    }

    pub fn cookies_per_ip_count(&self) -> usize {
        self.data.lock().unwrap().cookies_per_ip.len()
    }

    pub fn cookie_info_size() -> usize {
        std::mem::size_of::<SynCookieInfo>()
    }

    pub fn cookies_per_ip_size() -> usize {
        std::mem::size_of::<usize>()
    }

    pub fn container_info(&self) -> ContainerInfo {
        [
            (
                "syn_cookies",
                self.cookies_count(),
                Self::cookie_info_size(),
            ),
            (
                "syn_cookies_per_ip",
                self.cookies_per_ip_count(),
                Self::cookies_per_ip_size(),
            ),
        ]
        .into()
    }
}

impl Default for SynCookies {
    fn default() -> Self {
        Self::new(10)
    }
}

struct LockedSynCookies {
    cookies: HashMap<SocketAddrV6, SynCookieInfo>,
    cookies_per_ip: HashMap<Ipv6Addr, usize>,
}

impl LockedSynCookies {
    fn dec_cookie_count(&mut self, ip_addr: Ipv6Addr) {
        let ip_cookies = self.cookies_per_ip.entry(ip_addr).or_default();
        if *ip_cookies > 0 {
            *ip_cookies -= 1;
        } else {
            panic!("More SYN cookies deleted than created for IP");
        }
    }
}

struct SynCookieInfo {
    cookie: Cookie,
    created_at: Instant,
}

impl SynCookieInfo {
    fn exceeds_cutoff(&self, cutoff: Duration, now: Instant) -> bool {
        now.duration_since(self.created_at) > cutoff
    }
}
