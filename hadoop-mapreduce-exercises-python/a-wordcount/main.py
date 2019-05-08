import subprocess

if __name__ == "__main__":
  ps = subprocess.run('cat', '../input/words.txt')
  # output = subprocess.check_output(('grep', 'process_name'), stdin=ps.stdout)