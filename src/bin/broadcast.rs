use anyhow::Context;
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{StderrLock, StdoutLock, Write},
    sync::mpsc::Sender,
    time::Duration,
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

// enum for events
enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,

    // Map with info about nodes and their knowns neighbors
    known: HashMap<String, HashSet<usize>>,

    // Nodes whom to gossip with
    neighborhood: Vec<String>,
}

// Implementation of the trait Node for BroadcastNode
impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        sx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        // Threads that will send a gossip event every 300 milis til end of execution
        std::thread::spawn(move || {
            // Generate gossip events
            // Handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = sx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(BroadcastNode {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),

            neighborhood: Vec::new(),
        })
    }

    // fn step to act at any given message depending on its payload
    fn step<'a>(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        // Match for every possible event
        match input {
            Event::EOF => {
                todo!();
            }

            // If we get and Event we match which and then perform the task
            Event::Injected(payload) => match payload {
                // For a gossip, we send a message to every neighbor with the neigborhood each node knows
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip {
                                    seen: self
                                        .messages
                                        .iter()
                                        .copied()
                                        .filter(|m| !known_to_n.contains(m))
                                        .collect(),
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                    Ok(())
                }
            },
            Event::Message(input) => {
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
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk {};
                        // Serialize the rust struct into a json object with context in case of fail
                        reply.send(&mut *output).context("reply to broadcast")?;
                    }

                    Payload::Read { .. } => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        // Serialize the rust struct into a json object with context in case of fail
                        reply.send(&mut *output).context("reply to read")?;
                    }

                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("No topology given for node {}", self.node));
                        reply.body.payload = Payload::TopologyOk {};
                        reply.send(&mut *output).context("reply to topology")?;
                    }

                    // A way to group up different matches with the same handler
                    Payload::BroadcastOk { .. }
                    | Payload::ReadOk { .. }
                    | Payload::TopologyOk {} => {}
                }
                Ok(())
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop::<_, BroadcastNode, _, _>(());
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
