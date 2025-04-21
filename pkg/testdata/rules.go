package testdata

var TestSuccessSimpleRule1 = `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 10s
        event:
          source: kafka
        order:
          - value: "io.vertx.core.VertxException: Thread blocked"
            count: 3
`

var TestSuccessComplexRule2 = `
rules:
  - cre:
      id: TestSuccessComplexRule2
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3
terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      window: 1s
      event:
        source: k8s
      match:
        - field: "reason"
          value: "Killing"
        - field: "reason"
          value: "NodeShutdown"
      negate:
        - SIGTERM
  term3:
    sequence:
      window: 5s
      correlations:
        - hostname
      order:
        - sequence:
            window: 1s
            event:
              source: nginx
            order:
              - error message
              - shutdown
        - set:
            event:
              source: nginx
            match:
              - 90%
        - set:
            event:
              source: k8s
            match:
              - field: "reason"
                value: "Killing"
`

var TestSuccessComplexRule3 = `
rules:
  - cre:
      id: TestSuccessComplexRule3
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
terms:
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      match:
        - field: "reason"
          value: "Killing"
`

var TestSuccessComplexRule4 = `
rules:
  - cre:
      id: TestSuccessComplexRule4
    metadata:
      hash: 2KdXQZDAfRbYcH9FBDteBS
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term4
        negate:
          - term3

terms:
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  
  term2:
    sequence:
      window: 5s
      correlations:
        - container_id
      order:
        - sequence:
            window: 1s
            event:
              source: nginx
            order:
              - error message
              - shutdown
        - set:
            event:
              source: nginx
            match:
              - 90%
        - set:
            event:
              source: k8s
            match:
              - field: "reason"
                value: "Killing"
  term4:
    sequence:
      window: 5s
      correlations:
        - container_id
      order:
        - sequence:
            window: 1s
            event:
              source: nginx
            order:
              - error message
              - shutdown
        - set:
            event:
              source: nginx
            match:
              - 90%
        - set:
            event:
              source: k8s
            match:
              - field: "reason"
                value: "Killing"
  term3:
    set:
      event:
        source: k8s
      match:
        - field: "reason"
          value: "NodeShutdown"
`

var TestSuccessComplexRule5 = `
rules:
  - cre:
      id: TestSuccessComplexRule5
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
terms:
  term1:
    sequence:
      window: 10s
      event:
        src: log
        origin: true
        imageUrl: "*rabbitmq*"
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        src: k8s
      match:
      - field: "reason"
        value: "Killing"
`

var TestSuccessNegateOptions1 = `
rules:
  - cre:
      id: TestSuccessNegateOptions1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 10s
        event:
          source: kafka
        order:
          - value: "io.vertx.core.VertxException: Thread blocked"
            count: 3
        negate:
          - value: "SIGTERM"
            window: 10s
            slide: 1s
            anchor: 0
            abs: true
          - value: "SIGKILL"
            window: 10s
            slide: 1s
            anchor: 0
            abs: true
`

var TestSuccessNegateOptions2 = `
rules:
  - cre:
      id: TestSuccessNegateOptions2
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
        negate:
          - value: term3
            window: 10s
            slide: 1s
            anchor: 0
            abs: true

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: log
        origin: true
        image_url: "*rabbitmq*"
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      match:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: log
      match:
        - value: "Killing"
`

/* Failure cases */
var TestFailTypo = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailTypo
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      generation: 1
    rule:
      sequence:
        window: 10s
        event:
          source: kafka
        order:
          - regexs: "io.vertx.core.VertxException: Thread blocked"        # typo
`

var TestFailMissingOrder = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailMissingOrder
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      generation: 1
    rule:
      sequence:
        window: 10s
        event:
          source: kafka
        match:                                                            # cannot use match with sequence
          - regex: "io.vertx.core.VertxException: Thread blocked"
`

var TestFailMissingMatch = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailMissingMatch
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      generation: 1
    rule:
      set:
        window: 10s
        event:
          source: kafka
        order:                                                            # cannot use order with set
          - regex: "io.vertx.core.VertxException: Thread blocked"
`

var TestFailInvalidWindow = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailInvalidWindow
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      generation: 1
    rule:
      set:
        window: 10d                                                       # invalid window
        event:
          source: kafka
        match:
          - regex: "io.vertx.core.VertxException: Thread blocked"
`

var TestFailUnsupportedRule = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailInvalidWindow
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      generation: 1
    rule:
      superduperset:                                                       # unsupported rule type
        window: 10s
        event:
          source: kafka
        match:
          - regex: "io.vertx.core.VertxException: Thread blocked"
`

var TestFailMissingPositiveCondition = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailMissingPositiveCondition
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3
terms:
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      negate:
        - field: "reason"
          value: "NodeShutdown"
  term3:
    sequence:
      window: 5s
      correlations:
        - container_id
      order:
        - sequence:
            window: 1s
            event:
              source: nginx
            order:
              - error message
              - shutdown
        - set:
            event:
              source: nginx
            match:
              - 90%
        - set:
            event:
              source: k8s
            match:
              - field: "reason"
                value: "Killing"
`

var TestFailNegativeCondition1 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailNegativeCondition1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3
terms:
  term1:
    sequence:
      window: 10s
      event:
        src: log
        origin: true
        imageUrl: "*rabbitmq*"
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        src: k8s
      negate:
        - field: "reason"
          value: "NodeShutdown"
  term3:
    sequence:
      window: 5s
      correlations:
        - container_id
      order:
        - sequence:
            window: 1s
            event:
              src: log
              containerName: nginx
            order:
              - error message
              - shutdown
        - set:
            event:
              src: log
              containerName: nginx
            match:
              - 90%
        - set:
            event:
              src: k8s
            match:
              - field: "reason"
                value: "Killing"
`

var TestFailNegativeCondition2 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailNegativeCondition2
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
terms:
  term1:
    sequence:
      window: 10s
      event:
        src: log
        origin: true
        imageUrl: "*rabbitmq*"
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        src: k8s
      negate:
      - field: "reason"
        value: "Killing"
`

var TestFailNegateOptions3 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailNegateOptions3
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      match:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: k8s
      negate:
        - field: "reason"
          value: "Killing"
          window: 10s
          slide: 1s
          anchor: 0
          abs: true
`

var TestFailNegateOptions4 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailNegateOptions4
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
        negate:
          - term3

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      match:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: k8s
      negate:
        - field: "reason"
          value: "Killing"
          window: 10s
          slide: 1s
          anchor: 0
          abs: true
`

var TestFailTermsSyntaxError1 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailTermsSyntaxError
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      moooch:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: k8s
      negate:
        - field: "reason"
          value: "Killing"
          window: 10s
          slide: 1s
          anchor: 0
          abs: true
`

var TestFailTermsSyntaxError2 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailTermsSyntaxError
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    set:
      event:
        source: k8s
      window: 10d
      match:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: k8s
      negate:
        - field: "reason"
          value: "Killing"
          window: 10s
          slide: 1s
          anchor: 0
          abs: true
`

var TestFailTermsSemanticError1 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailTermsSemanticError1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 30s
        correlations:
          - hostname
        order:
          - term1
          - term2
          - term3

terms:        
  term1:
    sequence:
      window: 10s
      event:
        source: rabbitmq
        origin: true
      order:
        - value: Discarding message
          count: 10
        - Mnesia overloaded
      negate:
        - SIGTERM
  term2:
    sequence:
      event:
        source: k8s
      window: 1s
      order:
      - field: "reason"
        value: "Killing"
  term3:
    set:
      event:
        source: k8s
      match:
        - field: "reason"
          value: "Killing"
          window: 10s
          slide: 1s
          anchor: 0
          abs: true
`

var TestFailTermsSemanticError2 = ` # Line 1 starts here
rules:
  - cre:
      id: TestFailTermsSemanticError1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
      generation: 1
    rule:
      sequence:
        window: 0s
        correlations:
          - hostname
        order:
          - term1
`
