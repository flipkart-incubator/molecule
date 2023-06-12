import random
import hashlib
import yaml

def dummy_gen_repeats(args, pipeline_args):
  random.seed(args.seed)
  r_list = random.sample(range(2, 9), args.count)
  configs = dict()
  for r in r_list:
    pipeline_args['repeat'] = r
    c_hash = hashlib.md5(yaml.dump(pipeline_args).encode('utf-8')).hexdigest()
    configs[c_hash] = pipeline_args.copy()
  return configs

def dummy_gen_nums(args, pipeline_args):
  r_list = random.sample(range(1000, 9999), args.count)
  configs = dict()
  for r in r_list:
    pipeline_args['seed'] = r
    c_hash = hashlib.md5(yaml.dump(pipeline_args).encode('utf-8')).hexdigest()
    configs[c_hash] = pipeline_args.copy()
  return configs
