-- catalog-override: Check previously claimed native input batches before applying writes.
SELECT batch_num, status FROM {relation} ORDER BY batch_num
