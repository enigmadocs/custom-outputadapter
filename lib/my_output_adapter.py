import os

from parsekit.steps.load import OutputAdapter

class MyPipeOutputAdapter(OutputAdapter):
# A custom output adapter must subclass parsekit.steps.load.OutputAdapter

  def connect(self, creds):
     """No external resource to connect to """
     pass
     # A local file output adapter does not connect to any external resources,
     # so it is fine to just pass on this method.

  def disconnect(self, creds):
     """No external resource to export to """
     pass

  def create_table(self, table_name):
  # The create_table method takes a table name and creates
  # a table resource in the output. In this example, we want
  # to create a file handle to an output file with a name
  # that matches the table name. The repository option will
  # represent the path to this file.
     """Create a table, bucket, etc at the output."""
     output_path = os.path.join(self.repository, table_name)
     self.tables[table_name]['output_path'] = output_path
     self.tables[table_name]['fh'] = open(output_path, 'wb')
     header = self.tables[table_name].schema.field_names
     # Every output adapter has access to self.tables, a map
     # of table names to table objects. These table objects
     # can be used like Python dictionaries to store useful
     # information that is unique to that table, such as the
     # table's file handle and output path.
     self.write_record(header, table_name)
     self.log.info('Created table with schema: ' + unicode(header))
     # It would be nice if our output had a header row, so the
     # schema field names are used to create a header row in our
     # pipe-delimited file. We simply pass the field names as
     # a record to write_record so that they can be written to
     # file. Logging is a nice way to let users know what is
     # happening.

  def map_types(self, record, schema):
     """Coerce record to type representation appropriate for output."""
     return [unicode(field) for field in record]
     # Map types need to map all different Python/ParseKit types
     # to something safe for our output. In this case, let's
     # just use the default string representations of the classes by
     # calling unicode on every field and return that in a list.

  def write_record(self, encoded, table_name):
     """Write record to output"""
     self.tables[table_name]['fh'].write('|'.join(encoded))
     self.tables[table_name]['fh'].write('\n')
     # Output Adapters use write_records() to take an input record and
     # write it to a table. In this case, we can just the file handle
     # we stored in self.tables and write a pipe-delimited version of
     # the input record. Let's also add a new line after to make records
     # delimited by new lines.

  def close_table_complete(self, table_name):
     """Close table and commit data when parse concludes successfully."""
     self.tables[table_name]['fh'].close()
     # When the parser is completed, we need a way to commit the data.
     # With file handles, that's as simple as closing the file handle.

  def pause_table(self, table_name):
     """Pause connection to table, used if max connections exceeded."""
     self.close_table_complete(table_name)
     # File handles cannot really be paused, and writing to them
     # auto-commits. So, there is no custom logic to be written,
     # and instead we just close the file handle when we want to
     # pause writing, and close the file handle when an error occurs.

  def close_table_error(self, table_name):
     """Close table connection in exception or unexpected interruption."""
     self.close_table_complete(table_name)

  def reopen_table(self, table_name):
     """Reconnect to a paused table connection."""
     output_path = self.tables[table_name]['output_path']
     self.tables[table_name]['fh'] = open(output_path, 'a')
     # Reopening, however, is slightly different; we don't want to
     # lose the data that has already been written. So, let's open
     # the file in append mode in order to preserve previously written
     # data records.
