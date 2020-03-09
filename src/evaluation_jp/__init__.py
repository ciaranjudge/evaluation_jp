import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
print(str(Path(__file__).resolve().parent))
print("--------------------")
print('\n'.join(sys.path)) 