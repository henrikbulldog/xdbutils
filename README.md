# xdbutils
Extended dbutils (Databricks utilities)

# Databricks Connect
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#limitations

# PlanText
```
@startuml
skin rose
title Databricks Medallion Pipeline

class DataLakeHouse {
  +RawJob raw2bronze(system, class)
  +Bronze2SilverJob bronze2silver(system, class)
  +Silver2GoldJob silver2gold(system, class)
}

class Job {
  +void init(system, class)
  +{abstract}Reader read
}

class Raw2BronzeJob {
  +RawReader read(system, class, path, format)
}

class Bronze2SilverJob {
  +BronzeReader read(system, class, path, format)
}

class Silver2GoldJob {
  +SilverReader read(system, class, path, format)
}

class Reader {
  +Reader load(path, format)
  +Reader transform(callback)
  +Writer write
  #{abstract}void batch_handler(batch, epoch_id)
  -StreamReader get_stream_reader(source_path, format, checkpoint_location, **kwargs)
}

class Writer {
  +void merge(table, keys, path, partitions)
  +void append(table, path, partitions)
  +void overwrite(table, path, partitions)
  +void custom(callback)
}

class RawReader {
  #void batch_handler(batch, epoch_id)
}

class BronzeReader {
  #void batch_handler(batch, epoch_id)
}

class SilverReader {
  #void batch_handler(batch, epoch_id)
}

Job <|-down- Raw2BronzeJob: Inheritance
Job <|-down- Bronze2SilverJob: Inheritance
Job <|-down- Silver2GoldJob: Inheritance


Reader <|-down- RawReader: Inheritance
Reader <|-down- BronzeReader: Inheritance
Reader <|-down- SilverReader: Inheritance

@enduml
```
