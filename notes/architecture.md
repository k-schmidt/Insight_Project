# Architecture

Sychronization is a problem. There is no sense of shared time.
We could estimate the time and let the last write win.
Store a timestamp of every write but it will require us to have a good sense of what time it is.
Derive the time logically in vector clocks. Vector clocks don't tell us when but only what order events occur.
