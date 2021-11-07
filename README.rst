simulate network outage
---

- docker-compose up
- docker network ls
    - to get id of consumer network
- docker ps
    - to get id of consumer container
- docker network disconnect <consumer network> <consumer container>
- wait for some messages to pile up
- docker network connect <consumer network> <consumer container>