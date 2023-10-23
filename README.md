# Fly.io Distributed Challenges

Series of challenges proposed by [fly.io](https://fly.io/dist-sys/) done following the implementation of [Jon Gjengset](https://www.youtube.com/watch?v=gboGyccRVXI&t=10971s).

Challenges solved are:

- #1 Echo challenges

- #2 Unique ID Generation

- #3a Single node Broadcast  
  - #3b Multi node Broadcast
  - #3c Fault Tolerant Broadcast

Each challenge has its own branch inside the repo, containing its solution. The code is structured keeping in mind a generic approach:

- lib.rs contains common parts to every challenge.

-  inside `src/` there's a **challenge**.rs containing the specific implementation for each problem.

As for side exercise, there's an async implementation of every challenge up to #3, more info inside the code in the `async_third` branch.
