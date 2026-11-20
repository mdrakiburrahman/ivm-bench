-- catalog-override: Mark an entirely applied native input batch without implying dataflow hydration.
UPDATE {relation} SET status = 'applied' WHERE batch_num = %s AND status = 'started'
