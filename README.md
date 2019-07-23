# Recipe Analysis

In Recipe Analyis we are building a dynamic data pipeline build by ordering tasks in the right sequence. Tasks are unit of operation
with in the pipeline. All tasks must be a subclass of 'pipeline.task.Task' and override its execute method.

The tasks passed as ordered arguments to 'pipeline.executor.py' script.

An example argument:

<APP_NAME> HTTP_READ <file type> <HTTP/HTTPS path> RECIPES_TRANSFORM WRITE json file:///Users/hasif/Learning/recipe/ BEEF_RECIPE_TRANSFORM PARTITION_WRITE <file type> hdfs://<HDFS location> <partition column>

- **<APP_NAME>** - Application Name.
- **HTTP_READ <file format> <HTTP/HTTPS path>** - This task reads data from the Http/Https url, type of the data is also passed as an argument.
- **RECIPES_TRANSFORM** This task applies transformation given in this exercise to the entire dataset. 
- **WRITE <file format> file://<LOCAL_PATH>** This task writes the result from previous transformation to the given local path with the specified type.
- **BEEF_RECIPE_TRANSFORM** This task applies transformation given in this exercise to dataset after filtering the 'ingredients' column for keyword 'beef'.
- **PARTITION_WRITE <file format> hdfs://<HDFS location> <partition column>** Task writes to hdfs in the specified format and location.

For example
python executor.py ingest_recipe HTTP_READ json <INPUT_PATH> WRITE json file://<LOCAL_PATH>
python executor.py ingest_recipe HTTP_READ <INPUT_PATH> BEEF_RECIPE_TRANSFORM PARTITION_WRITE json hdfs://<HDFS_PATH> difficulty
python executor.py ingest_recipe HTTP_READ json <INPUT_PATH> RECIPES_TRANSFORM WRITE json file://<LOCAL_PATH> BEEF_RECIPE_TRANSFORM PARTITION_WRITE json hdfs://<HDFS_PATH> difficulty



Below lists the keywords associated with each tasks.

- "HDFS_READ": "pipeline.task.Reader",
- "HTTP_READ": "pipeline.task.HttpReader",
- "WRITE": "pipeline.task.Writer",
- "PARTITION_WRITE": "pipeline.task.PartitionWriter",
- "FILE_WRITE": "pipeline.task.Writer",
- "RECIPES_TRANSFORM": "pipeline.task.TransformRecipes",
- "BEEF_RECIPE_TRANSFORM": "pipeline.task.BeefRecipes",
- "EMAIL": "pipeline.task.EmailTask",
- "SLACK": "pipeline.task.SlackTask",
- "KAFKA": "pipeline.task.KafkaTask"