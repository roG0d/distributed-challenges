use anyhow::Context;
use async_trait::async_trait;
use rand::prelude::*;
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, Stdout},
    sync::Mutex,
    task::{self, JoinHandle},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
// Serde decorator to call Payload as type
#[serde(tag = "type")]
// Serde decorator to convert every Enum payload into snake_cases
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        #[serde(rename = "message")]
        message: usize,
    },
    BroadcastOk {},

    Read {},
    ReadOk {
        #[serde(rename = "messages")]
        messages: HashSet<usize>,
    },

    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {},

    // Gossip messages are fire-and-forget so we do not need a GossipOk
    Gossip {
        seen: HashSet<usize>,
    },
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,

    // Kinda memory for which messages to whom node we have sent to
    msg_history: HashMap<String, HashSet<usize>>,

    // Map with info about nodes and their knowns neighbors
    known: HashMap<String, HashSet<usize>>,

    // Nodes whom to gossip with
    neighborhood: Vec<String>,
}

// Implementation of the trait Node for BroadcastNode
#[async_trait]
impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let init_hashmaps: HashMap<String, HashSet<usize>> = init
            .node_ids
            .into_iter()
            .map(|nid| (nid, HashSet::new()))
            .collect();

        Ok(BroadcastNode {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init_hashmaps.clone(),
            msg_history: init_hashmaps.clone(),
            neighborhood: Vec::new(),
        })
    }

    // fn step to act at any given message depending on its payload
    async fn step<'a>(
        &mut self,
        input: Message<Payload>,
        output: &'a mut tokio::io::Stdout,
    ) -> anyhow::Result<()> {
        // Match for every possible event
        let mut reply = input.clone().into_reply(Some(&mut self.id));
        match reply.body.payload {
            // if we get a gossip payload, we extend the list of messages received and the map of known nodes
            Payload::Gossip { seen } => {
                self.known
                    .get_mut(&reply.dst)
                    .expect("got gossip from unknown node")
                    .extend(seen.iter().copied());
                self.messages.extend(seen);
            }
            Payload::Broadcast { message } => {
                reply.body.payload = Payload::BroadcastOk {};
                reply
                    .send(&mut *output)
                    .await
                    .context("reply to broadcast")?;

                if !self.messages.contains(&message) {
                    for neighbor in self.neighborhood.iter() {
                        if !self.msg_history[neighbor].contains(&message) {
                            let broadcast_reply = Message {
                                src: self.node.clone(),
                                dst: neighbor.clone(),
                                body: Body {
                                    id: Some(self.id),
                                    in_reply_to: input.body.id,
                                    payload: Payload::Broadcast { message },
                                },
                            };
                            broadcast_reply.send(&mut *output).await?;
                            self.msg_history.get_mut(neighbor).expect("Unknow node").insert(message);
                        }
                    }
                }
                self.messages.insert(message);
            }

            Payload::Read { .. } => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                // Serialize the rust struct into a json object with context in case of fail
                reply.send(&mut *output).await.context("reply to read")?;
            }

            Payload::Topology { mut topology } => {
                self.neighborhood = topology
                    .remove(&self.node)
                    .unwrap_or_else(|| panic!("No topology given for node {}", self.node));
                reply.body.payload = Payload::TopologyOk {};
                reply
                    .send(&mut *output)
                    .await
                    .context("reply to topology")?;
            }

            // A way to group up different matches with the same handler
            Payload::BroadcastOk { .. } | Payload::ReadOk { .. } | Payload::TopologyOk {} => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop::<_, BroadcastNode, _>(());
    Ok(())
}

// command to run malestron broadcast test, has to be on maelstrom file where maelstrom exe is (have to indicate the rust compilation target too)
// Single-node broadcast test command:
// ./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

//Testing neighbors command:
//./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --time-limit 5 --log-stderr

//Multi-node broadcast test command:
//./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 5 --time-limit 20 --rate 10

//Fault tolerant broadcast:
//./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
