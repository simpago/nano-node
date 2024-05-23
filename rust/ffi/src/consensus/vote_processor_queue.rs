use super::{rep_tiers::RepTiersHandle, VoteHandle};
use crate::{
    ledger::datastore::LedgerHandle, representatives::OnlineRepsHandle, transport::ChannelHandle,
    utils::ContainerInfoComponentHandle, StatHandle,
};
use rsnano_core::Vote;
use rsnano_node::{consensus::VoteProcessorQueue, transport::ChannelEnum};
use std::{
    collections::VecDeque,
    ffi::{c_char, CStr},
    ops::Deref,
    sync::Arc,
};

pub struct VoteProcessorQueueHandle(pub Arc<VoteProcessorQueue>);

impl Deref for VoteProcessorQueueHandle {
    type Target = Arc<VoteProcessorQueue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[no_mangle]
pub unsafe extern "C" fn rsn_vote_processor_queue_create(
    max_votes: usize,
    stats: &StatHandle,
    online_reps: &OnlineRepsHandle,
    ledger: &LedgerHandle,
    rep_tiers: &RepTiersHandle,
) -> *mut VoteProcessorQueueHandle {
    Box::into_raw(Box::new(VoteProcessorQueueHandle(Arc::new(
        VoteProcessorQueue::new(
            max_votes,
            Arc::clone(stats),
            Arc::clone(online_reps),
            Arc::clone(ledger),
            Arc::clone(rep_tiers),
        ),
    ))))
}

#[no_mangle]
pub unsafe extern "C" fn rsn_vote_processor_queue_destroy(handle: *mut VoteProcessorQueueHandle) {
    drop(Box::from_raw(handle))
}

#[no_mangle]
pub extern "C" fn rsn_vote_processor_queue_len(handle: &VoteProcessorQueueHandle) -> usize {
    handle.0.len()
}

#[no_mangle]
pub extern "C" fn rsn_vote_processor_queue_is_empty(handle: &VoteProcessorQueueHandle) -> bool {
    handle.0.is_empty()
}

#[no_mangle]
pub extern "C" fn rsn_vote_processor_queue_vote(
    handle: &VoteProcessorQueueHandle,
    vote: &VoteHandle,
    channel: &ChannelHandle,
) -> bool {
    handle.0.vote(vote, channel)
}

#[no_mangle]
pub extern "C" fn rsn_vote_processor_queue_flush(handle: &VoteProcessorQueueHandle) {
    handle.0.flush();
}

#[no_mangle]
pub extern "C" fn rsn_vote_processor_queue_stop(handle: &VoteProcessorQueueHandle) {
    handle.0.stop();
}

#[no_mangle]
pub unsafe extern "C" fn rsn_vote_processor_collect_container_info(
    handle: &VoteProcessorQueueHandle,
    name: *const c_char,
) -> *mut ContainerInfoComponentHandle {
    Box::into_raw(Box::new(ContainerInfoComponentHandle(
        handle
            .0
            .collect_container_info(CStr::from_ptr(name).to_string_lossy().to_string()),
    )))
}
