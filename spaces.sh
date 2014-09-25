hyperdex add-space << EOF
    space messages
    key id
    attributes
        string body
EOF
echo added space messages

hyperdex add-space << EOF
    space handles
    key id
    attributes
        string messageId
EOF
echo added space handles 

# TODO handle pointer
