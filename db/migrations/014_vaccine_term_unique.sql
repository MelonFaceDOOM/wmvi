-- didn't realize this wasn't already unique
ALTER TABLE taxonomy.vaccine_term
ADD CONSTRAINT vaccine_term_name_unique UNIQUE (name);
