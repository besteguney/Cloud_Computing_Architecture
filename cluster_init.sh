kops create -f part1.yaml
kops create secret --name part1.k8s.local sshpublickey admin -i ~/.ssh/cloud-computing.pub
kops update cluster --name part1.k8s.local --yes --admin
kops validate cluster --wait 10m
