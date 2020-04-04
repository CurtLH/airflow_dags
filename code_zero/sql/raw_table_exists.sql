SELECT EXISTS (
  SELECT FROM information_schema.tables
  WHERE  table_schema = 'bedpage'
  AND    table_name   = 'raw'
);
