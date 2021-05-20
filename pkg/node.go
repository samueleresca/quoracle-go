package pkg

import (
    "fmt"
    "time"
)


type Node struct {
    Name string
    ReadCapacity *float64
    WriteCapacity *float64
    Latency *time.Time
}

func DefNode(name string, capacity *float64, readCapacity *float64, writeCapacity *float64, latency *time.Time) Node {
    node := Node{}
    node.Name = name

    if capacity == nil && readCapacity == nil && writeCapacity == nil {
        initialValue := 1.0
        node.ReadCapacity = &initialValue

    }else if capacity != nil && readCapacity == nil && writeCapacity == nil{
        node.ReadCapacity = capacity
        node.WriteCapacity = capacity
    }else if capacity == nil && readCapacity != nil && writeCapacity != nil {
        node.ReadCapacity = readCapacity
        node.WriteCapacity = writeCapacity
    }else{
        panic("You must specify capacity or (read_capacity 'and write_capacity)")
    }

    if latency == nil{
        oneSec := time.Date(0,0,0,0,0,1,0, nil)
        latency = &oneSec
    }
    return node
}

func (n *Node) String() string {
    return n.Name
}



func (n *Node) Quorums() chan map[GenericExpr]bool {
    chnl := make(chan map[GenericExpr]bool)

    go func() {
        chnl <- map[GenericExpr]bool{n : true}
        // Ensure that at the end of the loop we close the channel!
        close(chnl)
    }()

    return chnl
}

func (n *Node) IsQuorum(xs map[GenericExpr]bool) bool {
    var found = false
    for  k, _ := range xs {
        if n.String() == k.String() {
            found = true
            return found
        }
    }
    return found
}


func (n *Node) Nodes() map[Node]bool {
   return map[Node]bool { *n : true }
}

func (n *Node) Dual() GenericExpr {
   return n
}


func (n *Node) NumLeaves() int {
    return 1
}

func (n *Node) DupFreeMinFailures() int {
    return 1
}




type GenericExpr interface {
   Add(expr GenericExpr) GenericExpr
   Multiply(expr GenericExpr) GenericExpr
   Quorum() chan map[GenericExpr]bool
   IsQuorum(map[GenericExpr]bool) bool
   Nodes() map[Node]bool
   NumLeaves() int
   DupFreeMinFailures() int
   String() string
}


type Expr struct {
    string Name


}

func (lhs Expr) Add(rhs GenericExpr) GenericExpr {
   return lhs.Add(rhs)
}

func (lhs Expr) Multiply(rhs GenericExpr) GenericExpr {
   return lhs.Multiply(rhs)
}



type Or struct {
   Es []GenericExpr
}

func (expr *Or) NumLeaves() int {
    total := 0

    for _, e := range expr.Es {
        total += e.NumLeaves()
    }

    return total
}

func (expr *Or) DupFreeMinFailures() int {
    total := 0

    for _, e := range expr.Es {
        total += e.DupFreeMinFailures()
    }

    return total
}

func (expr *Or) Add(e GenericExpr) GenericExpr {
    panic("implement me")
}

func (expr *Or) Multiply(e GenericExpr) GenericExpr {
    panic("implement me")
}

func (expr *Or) Quorum()  chan map[GenericExpr]bool {
    chnl := make(chan map[GenericExpr]bool)
    go func() {
        for _, e := range expr.Es {
            tmp := <- e.Quorum()
            chnl <- tmp

        }
        // Ensure that at the end of the loop we close the channel!
        close(chnl)
    }()
    return chnl
}

func (expr *Or) IsQuorum(xs map[GenericExpr]bool) bool {
    var found = true
    for  _, e := range expr.Es {
        if !e.IsQuorum(xs) {
            found = false
            return found
        }
    }
    return found
}

func (expr *Or) Nodes() map[Node]bool {
    var final = make(map[Node]bool)

    for _, e := range expr.Es {
        for n := range e.Nodes() {
            final[n] = true
        }
    }
    return final
}

func (expr *Or) String() string {
   return fmt.Sprintf("%b", expr.Es)
}

type And struct {
   Es []GenericExpr
}

func (expr *And) String() string {
   return fmt.Sprintf("%b", expr.Es)
}

func (expr *And) Quorums() chan map[GenericExpr]bool {
   chnl := make(chan map[GenericExpr]bool)
   go func() {
      for _, e := range expr.Es {
          tmp := <- e.Quorum()
          chnl <- tmp
      }

      // Ensure that at the end of the loop we close the channel!
      close(chnl)
   }()
   return chnl
}

func (expr *And) IsQuorum(xs map[GenericExpr]bool) bool {
 var found = false
 for  _, e := range expr.Es {
     if e.IsQuorum(xs) {
        found = true
        return found
     }
   }
   return found
}


func (expr *And) Nodes() map[Node]bool {
    var final = make(map[Node]bool)

    for _, e := range expr.Es {
        for n := range e.Nodes() {
            final[n] = true
        }
    }
    return final
}

func (expr *And) Dual() GenericExpr {
    return &Or{expr.Es}
}


func (expr *And) NumLeaves() int {
    total := 0

    for _, e := range expr.Es {
        total += e.NumLeaves()
    }

    return total
}

func (expr *And) DupFreeMinFailures() int {
    var exprs = expr.Es
    var min = exprs[0].DupFreeMinFailures()

    for _, expr := range exprs {
        if min > expr.DupFreeMinFailures() {
            min = expr.DupFreeMinFailures()
        }
    }
    return min
}



