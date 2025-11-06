# udp-log-parser
Parses logs received over UDP datagram. Configurable log schemas for PostgreSQL table insertion


# setup




# architecture
┌─────────────┐
│  UDP Client │──────┐
└─────────────┘      │
                     ▼
┌─────────────┐    ┌──────────────┐
│  UDP Client │───▶│  UDP Socket  │
└─────────────┘    │   Port 8125  │
                   └──────┬───────┘
                          │
                    ┌─────▼─────┐
                    │Log Parser │
                    │  Process  │
                    └─────┬─────┘
                          │ Parses & Routes
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │ Queue 1 │      │ Queue 2 │      │ Queue N │
   └────┬────┘      └────┬────┘      └────┬────┘
        │                │                 │
   ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
   │Workers  │      │Workers  │      │Workers  │
   │(1 to n) │      │(1 to n) │      │(1 to n) │
   └────┬────┘      └────┬────┘      └────┬────┘
        │                │                 │
        └────────────────┼─────────────────┘
                         ▼
                  ┌──────────┐
                  │PostgreSQL│
                  └──────────┘
        