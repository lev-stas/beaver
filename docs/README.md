# BEAVER Steps

This directory describes a sequence of infrastructure states called **STEPs**.
Each STEP is a ready-to-use starting point for experiments with the BEAVER
stand. It explains the state of the infrastructure, the problems worth
exploring, and a possible direction for the next iteration.

STEPs are recommendations, not a mandatory course or a strict upgrade path.
You can follow them in order, skip directly to any published STEP, combine
ideas from several STEPs, or ignore them and change the stand in any way that
is useful for your own experiments.

## How to Use a STEP

1. Choose a STEP from the sequence below.
2. Check out its Git tag to get the corresponding initial state.
3. Read the STEP's `README.md` to understand the infrastructure and suggested
   goals.
4. Design and implement your own changes.
5. Use `SOLUTION.md` only when you want an example or an additional reference.

For example, after the STEP has been published:

```bash
git switch --detach step-00
```

Create your own branch if you want to keep changes made from that state:

```bash
git switch -c my-step-00-experiments step-00
```

## STEP Directory Structure

Each STEP is stored in `steps/step-XX-<name>/` and contains two documents:

- `README.md` describes the initial infrastructure, the problems considered
  in this STEP, the desired outcomes, and how the proposed work may improve
  the stand.
- `SOLUTION.md` provides one possible step-by-step implementation or operating
  procedure.

The solution is not authoritative and is not guaranteed to be the best or
even a fully correct answer. It exists as an example and a reference point for
your own investigation. Evaluate its trade-offs, test its assumptions, break
it, and replace it when you find a better approach.

## STEP Sequence

| STEP | Initial state | Suggested goal | Documentation |
|---|---|---|---|
| STEP 00 | Project source and single-node Compose topology | Deploy and verify the stand | [Project Initialization](steps/step-00-initialization/README.md) |

Each published Git tag freezes both the infrastructure and its documentation.
Later STEPs may build on earlier work, but every STEP should remain usable as
an independent starting checkpoint.
