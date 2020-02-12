# Trade Statistics API

## About

Scripts to run our API to provide data in JSON format. This project features an extensive documentation available at https://docs.tradestatistics.io/.

## Setup

1. Edit `tradestatistics-api.service` if required.
2. Copy the file to `/etc/systemd/system/`.
3. Run `sudo systemctl start tradestatistics-api`.
4. Don't forget to add a `.Renviron` file with credentials or it won't work