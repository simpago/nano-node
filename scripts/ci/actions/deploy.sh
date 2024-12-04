#!/bin/bash

set -o errexit
set -o nounset
set -o xtrace
OS=$(uname)
IS_RPM_DEPLOY="${LINUX_RPM:-0}"
S3_BUCKET_NAME="${S3_BUCKET_NAME:-repo.nano.org}"

case "${NETWORK}" in
  "BETA")
      BUILD="beta"
      ;;
  "TEST")
      BUILD="test"
      ;;
  *)
      BUILD="live"
      ;;
esac

if [[ -n "${S3_BUILD_DIRECTORY}" ]]; then


    DIRECTORY="${S3_BUILD_DIRECTORY}/${BUILD}"
else
    DIRECTORY=$BUILD
fi

if [[ "$OS" == 'Linux' && "$IS_RPM_DEPLOY" -eq "1" ]]; then
    RPMS=$(find ${GITHUB_WORKSPACE}/artifacts/RPMS -type f -name '*.rpm')
    SRPMS=$(find ${GITHUB_WORKSPACE}/artifacts/SRPMS -type f -name '*.src.rpm')

    for rpm in $RPMS; do
        SHA=$(sha256sum ${rpm})
        echo "Hash: $SHA"
        echo $SHA > ${GITHUB_WORKSPACE}/$(basename "${rpm}.sha256")

        aws s3 cp ${rpm} s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/$(basename "${rpm}") --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
        aws s3 cp ${GITHUB_WORKSPACE}/$(basename "${rpm}.sha256") s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/$(basename "${rpm}.sha256") --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    done
    
    for srpm in $SRPMS; do
        SHA=$(sha256sum ${srpm})
        echo "Hash: $SHA"
        echo $SHA > ${GITHUB_WORKSPACE}/$(basename "${srpm}).sha256")

        aws s3 cp ${srpm} s3://${S3_BUCKET_NAME}/$DIRECTORY/source/$(basename "${srpm}") --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
        aws s3 cp ${GITHUB_WORKSPACE}/$(basename "${srpm}).sha256") s3://${S3_BUCKET_NAME}/$DIRECTORY/source/$(basename "${srpm}.sha256") --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    done
elif [[ "$OS" == 'Linux' ]]; then
    SHA=$(sha256sum $GITHUB_WORKSPACE/build/nano-node-*-Linux.tar.bz2)
    echo "Hash: $SHA"
    echo $SHA >$GITHUB_WORKSPACE/nano-node-$TAG-Linux.tar.bz2.sha256

    SHA=$(sha256sum $GITHUB_WORKSPACE/build/nano-node-*-Linux.deb)
    echo "Hash: $SHA"
    echo $SHA >$GITHUB_WORKSPACE/nano-node-$TAG-Linux.deb.sha256

    aws s3 cp $GITHUB_WORKSPACE/build/nano-node-*-Linux.tar.bz2 s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Linux.tar.bz2 --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    aws s3 cp $GITHUB_WORKSPACE/nano-node-$TAG-Linux.tar.bz2.sha256 s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Linux.tar.bz2.sha256 --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    aws s3 cp $GITHUB_WORKSPACE/build/nano-node-*-Linux.deb s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Linux.deb --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    aws s3 cp $GITHUB_WORKSPACE/nano-node-$TAG-Linux.deb.sha256 s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Linux.deb.sha256 --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
else
    SHA=$(sha256sum $GITHUB_WORKSPACE/build/nano-node-*-Darwin.dmg)
    echo "Hash: $SHA"
    echo $SHA >$GITHUB_WORKSPACE/build/nano-node-$TAG-Darwin.dmg.sha256

    aws s3 cp $GITHUB_WORKSPACE/build/nano-node-*-Darwin.dmg s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Darwin.dmg --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    aws s3 cp $GITHUB_WORKSPACE/build/nano-node-$TAG-Darwin.dmg.sha256 s3://${S3_BUCKET_NAME}/$DIRECTORY/binaries/nano-node-$TAG-Darwin.dmg.sha256 --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
fi
