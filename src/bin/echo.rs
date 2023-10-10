use anyhow::Context;
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
// Serde decorator to call Payload as type
#[serde(tag = "type")]
// Serde decorator to convert every Enum payload into snake_cases
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

// Implementation of the trait Node for EchoNode
impl Node<(), Payload> for EchoNode {
    fn from_init(
        _state: (),
        _init: Init,
        _sx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }

    // fn step to act at any given message depending on its payload
    fn step<'a>(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {

        // We assure that the Event we receive is an message type one, not a injected
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        match input.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop::<_, EchoNode, _, _>(());
    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/echo --node-count 1 --time-limit 10
