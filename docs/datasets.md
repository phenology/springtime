<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

Springtime brings together datasets from various sources. Where possible, we use
existing tools to retrieve the data. The flowchart below illustrates the various
sources. Note that there might be multiple ways/tools to get data from the same
source. In springtime this is perfectly fine. We just accomodate and explain how
each of them works. It is up to the user to make an informed decision on what
way is right for them.

```mermaid
flowchart TD

    %% Field observations
    subgraph Field observations
    F3[NEON]
    F1[PEP725]
    F2[NPN]
    F4[PPO]
    F5[rppo]
    F6[Phenocam]
    F7[PhenocamR]
    F8[rnpn]
    end

    %% Meteorology
    subgraph Meteorology
    M1[Daymet]
    M2[Single Pixel Tool]
    M3[Thredds]
    M4[DaymetR]
    %% M5[E-OBS]
    %% M6[cdsapi]
    end

    %% Satellites
    subgraph Satellites
    %% S1[Sentinel]
    S2[MODIS]
    S3[Land products subsets]
    S4[ModisTools]
    end

    %% packages
    P1[Phenor]

    %% connections
    M1 --> M2 & M3 --> M4
    %% M5 ---> M6
    F1 & F2 & F3 --> F4
    F2 --> F8
    S2 --> S3 --> S4
    F6 --> F7
    F4 ---> F5
    S4 & M4 & F7 -.-> P1
    F1 & F2 ----> P1

    %% Springtime
    S1[Springtime]
    S4 & M4 & F7 & F5 & P1 --> S1
    F8 --> S1
```
