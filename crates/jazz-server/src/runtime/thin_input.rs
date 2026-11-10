//! Host-independent thin request assembly. Hosts own admission and timers.
use super::ThinIngress;
use jazz::thin::{ThinAssembler, ThinRequest};
use jazz::wire::{WireError, WireErrorCode, WireFrame, WireRetry};

/// Inactivity allowed between fragments; hosts schedule their own wakeup.
#[cfg(not(test))]
pub const THIN_PART_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
#[cfg(test)]
pub const THIN_PART_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

pub fn malformed(message: impl ToString) -> WireError {
    WireError::new(
        WireErrorCode::MalformedFrame,
        WireRetry::Never,
        message.to_string(),
    )
}

/// Holds the admission reservation across parts, transferring it to the
/// completed request. Dropping the connection releases an unfinished request.
#[derive(Default)]
pub struct ThinRequestInput {
    assembler: ThinAssembler,
    room: Option<ThinIngress>,
}
impl ThinRequestInput {
    pub fn is_joining(&self) -> bool {
        self.assembler.is_joining()
    }

    /// Reserve the full advertised size before buffering its first part.
    pub fn reservation(
        &self,
        frame: &WireFrame,
        len: usize,
    ) -> Result<Option<(usize, bool)>, WireError> {
        match frame {
            WireFrame::ThinPart(part) if !self.is_joining() => {
                Ok(Some((part.total().map_err(malformed)?, true)))
            }
            WireFrame::ThinPart(_) => Ok(None),
            _ if self.is_joining() => Err(malformed("a thin frame arrived inside a split one")),
            WireFrame::ThinRequest(_) => Ok(Some((len, false))),
            _ => Err(malformed("a thin connection sends only thin requests")),
        }
    }

    pub fn push(
        &mut self,
        frame: WireFrame,
        room: Option<ThinIngress>,
    ) -> Result<Option<(ThinRequest, ThinIngress)>, WireError> {
        let (frame, room) = match frame {
            WireFrame::ThinPart(part) => {
                if !self.is_joining() {
                    self.room = room;
                }
                let Some(bytes) = self.assembler.push(part).map_err(malformed)? else {
                    return Ok(None);
                };
                let room = self
                    .room
                    .take()
                    .expect("split request admitted before assembly");
                (
                    jazz::wire::decode_postcard_frame(&bytes)
                        .map_err(|_| malformed("a split thin frame does not decode"))?,
                    room,
                )
            }
            frame => (frame, room.expect("whole request admitted before dispatch")),
        };
        match frame {
            WireFrame::ThinRequest(request) => Ok(Some((request, room))),
            _ => Err(malformed("a thin connection sends only thin requests")),
        }
    }
}
