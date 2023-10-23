use std::sync::Arc;

use anyhow::Context;
use rustengan::*;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncWriteExt, Stdout},
    sync::Mutex,
    task::{self, JoinHandle},
};
use async_trait::async_trait;


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
#[async_trait]
impl Node<(), Payload> for EchoNode {
    async fn from_init<'a>(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }

    // fn step to act at any given message depending on its payload
    async fn step<'a>(&mut self, input: Message<Payload>, output:  &'a mut Arc<Mutex<Stdout>>) -> anyhow::Result<()> {
        let mut output_clone = Arc::clone(output);

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
                let _ = reply.send(&mut output_clone).await;
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }

}

fn main() -> anyhow::Result<()> {
    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop::<_, EchoNode, _>(());
    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/echo --node-count 1 --time-limit 10
