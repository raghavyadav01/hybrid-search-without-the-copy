P=$1 ES=$2 OS=$3; cd ~/eval; O=results/ec2_mp_fix.json
B="python3 mpbench.py --pinot $P --es $ES --os $OS --n 1000 --warm 200 --conc 1,8,32,64 --out $O"
$B --cases pinot:bm25:esci_et:100,pinot:vector:esci_et:100,pinot:hybrid:esci_et:100,pinot:vector:esci_big:100,pinot:vector:esci_big:50,pinot:hybrid:esci_big:100
$B --cases pinot:vector_brand:esci_brand:100,pinot:hybrid_brand:esci_brand:100,pinot:vector_brand:esci_brand:20
echo MATRIX-DONE
