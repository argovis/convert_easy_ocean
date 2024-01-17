Some run notes:

 - `process_easy_ocean_gridded_data.py` to generate the raw json
   - dependencies described in Dockerfile
   - mind the directory structures; some may need to be created a level up
 - `populate_easyocean.py` to munge the raw json into its final form and populate a mongodb
   - same containerized environment as above
