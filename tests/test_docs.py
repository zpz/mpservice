import subprocess
from pathlib import Path


def test_docs():
    p = Path(__file__)
    p_docs = p.parent.parent / 'docs'
    print('\n... running doctest ...')
    subprocess.run(['make', 'doctest'], cwd=p_docs, check=True)
    print('\n... building documentation ...')
    subprocess.run(['make', 'html', 'SPHINXOPTS=-W'], cwd=p_docs, check=True)
    # If build fails, this will raise exception.