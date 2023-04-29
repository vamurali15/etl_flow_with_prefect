from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from ingest_to_gcs import new_parent_flow
docker_container_block = DockerContainer.load("prefect-zoomcamp")
docker_dep = Deployment.build_from_flow(
    flow= new_parent_flow,
    name = "new_etl_docker_flow",
    infrastructure=docker_container_block,
    )
if __name__ == "__main__":
    docker_dep.apply()

