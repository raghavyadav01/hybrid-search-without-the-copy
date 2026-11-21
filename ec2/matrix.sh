P=$1 ES=$2 OS=$3; cd ~/eval; O=results/ec2_mp.json
B="python3 mpbench.py --pinot $P --es $ES --os $OS --n 1000 --warm 200 --conc 1,8,32,64 --out $O"
$B --cases es:bm25,os:bm25,es:vector,os:vector,es:hybrid,os:hybrid --nc 1000
$B --cases es:vector,os:vector,es:hybrid,os:hybrid --nc 200
$B --cases es:vector_brand,os:vector_brand,es:hybrid_brand,os:hybrid_brand --nc 1000
$B --cases pinot:bm25:esci_et:100,pinot:vector:esci_et:100,pinot:hybrid:esci_et:100,pinot:vector:esci_big:100,pinot:vector:esci_big:50,pinot:hybrid:esci_big:100
$B --cases pinot:vector_brand:esci_brand:100,pinot:hybrid_brand:esci_brand:100,pinot:vector_brand:esci_brand:20
echo MATRIX-DONE
