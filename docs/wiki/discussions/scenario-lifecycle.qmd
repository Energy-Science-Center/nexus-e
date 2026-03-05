---
title: Scenario lifecycle
---


This is a guide to handle Nexus-e scenario storage.

About Nexus-e scenarios:

- When you upload a scenario it is stored as a MySQL database (input database) on a MySQL server.
- When you run a simulation, a temporary copy of this database (input copy) is created for the needs of the simulation and is then deleted after the simulation success.
- Some of the results of a simulation are uploaded to another database (output database) which is used by Nexus-e webviewer. Each simulation produce a new output database.

We ask the users to follow these rules:

- Delete the output databases that:
  * you don't want to keep
  * and are not involved in a publication
  * and are not used by a publicly shared webviewer link
- Delete the input databases that:
  * won't be used by foreseeable simulations
  * have been properly archived if involved in a publication or a publicly shared webviewer link
- Make sure to clean your input and output databases:
  * based on the rules above
  * at least once after the end of the project involving the databases
  * ideally every time you know some databases are not relevant anymore
- It can happen that the input copies are not automatically deleted when simulations end unintentionally. Please delete them manually, their names are based on the pattern `[input database name]_[timestamp]_[user initials]` and might contain a trailing string of random characters.
- Before deleting databases, you can choose to download them for archiving purpose, this is your responsibility. Please contact the Nexus-e team if you want some databases to be saved in the Long Term Storage solution offered by ETHZ **for archiving purpose only**.