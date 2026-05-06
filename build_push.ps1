# Build and push CreateZoom Docker image to EBRAINS registry
$ErrorActionPreference = "Stop"

$IMAGE = "docker-registry.ebrains.eu/workbench/deepzoom"




try {
    Write-Host "Building $IMAGE`:$VERSION..." -ForegroundColor Cyan
    docker build -t "${IMAGE}" -t "${IMAGE}:latest" .
    
    Write-Host "Pushing $IMAGE`:$VERSION..." -ForegroundColor Cyan
    docker push "${IMAGE}"
    docker push "${IMAGE}:latest"

}
catch {
    Write-Host "An error occurred: $_" -ForegroundColor Red
}

Write-Host "Done!" -ForegroundColor Green
