#/bin/bash

NAME=$(kubectl get pods  | grep -E 'migration-namager' | awk '{print $1}')

echo "Exec Into '$NAME'"

#kubectl exec -it $NAME -n $NS /bin/sh
kubectl logs -f $NAME

