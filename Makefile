up-infra:
	docker-compose up rabbit postgres minio flower

up-work1:
	docker-compose up worker-generic

up-work2:
	docker-compose up worker-gpu

up-dagit:
	docker-compose up dagit

