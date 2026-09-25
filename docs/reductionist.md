# CMS Reductionist Service Deployment

Meeting on 15th December 2025

## Present
- Bryan Lawrence
- Valeriu Predoi
- Stig Telfer
- Max Norton
- Daniel Westwood
- David Hassell

## Objectives
- Access Control - Time limited token linked per-chunk

## General Notes
- Wacasoft Project Funding (Kanban Board)
- Chunk Caching
    - Auth mechanism switched off currently.
    - uuid per chunk (not implemented)
- AIVAL2: Http Backend via range-get
    - No auth needed for this component
- AIVAL3: Filter upgrades
    - Varying types of compression

## Deployment on JASMIN
- Two machines with 2x(25-100)Gb/s networks - Will Furnell
- Currently deployed directly on physical machines (not VM) - using podman with Max Norton's user ID
    - Extend to a general 'reductionist' user instead - Dedicated account.
    - Manual startup on machine power-cycle.
    - Additional work: Use systemd to launch automatically when machine restarts.
    - Bought physical disks for the cache (high performance)
    - 2x 7TB SSD on each machine (RAID-1) mainly for chunk cache.
    - ESGF Funding (eventually) for time worked.
    - Idea to get this inside the ESGF data nodes.
- Possible Deployments
    - Access to server stack (multiple components in same server)
    - Separate components deployed individually.
    - Deploying reductionist on the same server (kubernetes deployment)
    - AIVAL4: Local Posix access - Kubernetes containerised deployment.
