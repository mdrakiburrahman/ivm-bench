-- catalog-override: Claim native input batches before any potentially uncertain write.
INSERT INTO {relation} (batch_num, status) VALUES (%s, 'started')
