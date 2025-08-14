//! # TLV Stream Writer
//!
//! Asynchronous, pull-based producer that encodes Type–Length–Value frames and yields them
//! as a `futures_core::Stream` of buffers. Uses the generic [`StreamBuffer`] so you can emit
//! either standard `Vec<u8>` or SIMD-aligned `Vec64<u8>` buffers. Push frames with
//! [`TLVStreamWriter::write_frame`], then call [`TLVStreamWriter::finish`] to signal end of stream.
//!
//! No alignment padding is applied.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::models::encoders::tlv::protocol::TLVEncoder;
use crate::models::frames::tlv_frame::TLVFrame;
use crate::traits::frame_encoder::FrameEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use futures_core::Stream;

/// Streaming TLV writer. Buffers encoded TLV frames and emits them as the supplied StreamBuffer.
pub struct TLVStreamWriter<B: StreamBuffer> {
    out_frames: VecDeque<B>,
    finished: bool,
    waker: Option<Waker>,
    global_offset: usize,
}

impl<B: StreamBuffer + 'static> TLVStreamWriter<B> {
    /// Create a new TLV streaming writer.
    pub fn new() -> Self {
        Self {
            out_frames: VecDeque::new(),
            finished: false,
            waker: None,
            global_offset: 0,
        }
    }

    /// Queue a new TLV frame for emission.
    pub fn write_frame(&mut self, t: u8, value: &[u8]) -> io::Result<()> {
        let frame = TLVFrame { t, value };
        let (encoded, _) = TLVEncoder::encode::<B>(&mut self.global_offset, &frame)?;
        self.out_frames.push_back(encoded);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Mark the stream as finished (EOS).
    pub fn finish(&mut self) {
        self.finished = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// Implements `Stream` to yield encoded TLV frames in order until `finish` is called.
impl<B> Stream for TLVStreamWriter<B>
where
    B: StreamBuffer + Unpin + 'static,
{
    type Item = io::Result<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(frame) = this.out_frames.pop_front() {
            Poll::Ready(Some(Ok(frame)))
        } else if this.finished {
            Poll::Ready(None)
        } else {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_core::Stream;
    use minarrow::Vec64;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    // Dummy waker for polling
    fn dummy_waker() -> Waker {
        fn no_op(_: *const ()) {}
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| dummy_raw_waker(), no_op, no_op, no_op);
        fn dummy_raw_waker() -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe { Waker::from_raw(dummy_raw_waker()) }
    }

    #[test]
    fn test_tlv_stream_writer_basic() {
        let mut writer = TLVStreamWriter::<Vec64<u8>>::new();

        // Write several TLV frames
        writer.write_frame(1, &[0x01, 0x02]).unwrap();
        writer.write_frame(2, &[0xAB, 0xCD, 0xEF]).unwrap();

        // Finish the stream
        writer.finish();

        // Pin writer for stream interface
        let mut pin_writer = Pin::new(&mut writer);

        // Construct a dummy context for polling
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);

        // First frame: type 1, value [0x01, 0x02]
        match pin_writer.as_mut().poll_next(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => {
                assert_eq!(frame[0], 1); // Type field
                assert_eq!(&frame[1..5], &(2u32.to_le_bytes())); // Length = 2
                assert_eq!(&frame[5..7], &[0x01, 0x02]); // Value
            }
            _ => panic!("expected frame 1"),
        }

        // Second frame: type 2, value [0xAB, 0xCD, 0xEF]
        match pin_writer.as_mut().poll_next(&mut cx) {
            Poll::Ready(Some(Ok(frame))) => {
                assert_eq!(frame[0], 2); // Type field
                assert_eq!(&frame[1..5], &(3u32.to_le_bytes())); // Length = 3
                assert_eq!(&frame[5..8], &[0xAB, 0xCD, 0xEF]); // Value
            }
            _ => panic!("expected frame 2"),
        }

        // End of stream
        match pin_writer.as_mut().poll_next(&mut cx) {
            Poll::Ready(None) => {}
            _ => panic!("expected stream end"),
        }
    }

    #[test]
    fn test_tlv_stream_writer_empty_finish() {
        let mut writer = TLVStreamWriter::<Vec64<u8>>::new();
        writer.finish();
        let mut pin_writer = Pin::new(&mut writer);
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);
        match pin_writer.as_mut().poll_next(&mut cx) {
            Poll::Ready(None) => {}
            _ => panic!("expected immediate end for empty writer"),
        }
    }
}
