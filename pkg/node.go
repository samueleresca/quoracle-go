package pkg

import (
    "fmt"
    "github.com/lanl/clp"
    "math"
    "strings"
    "time"
)


type Node struct {
    Name string
    ReadCapacity *float64
    WriteCapacity *float64
    Latency *time.Time
}

func (n Node) GetEs() []GenericExpr {
    return []GenericExpr {n}
}

func (n Node) Expr() string {
    return "Node"
}

func (n Node) Add(expr GenericExpr) Or {
    return orExpr(n, expr)
}

func (n Node) Multiply(expr GenericExpr) And {
    return andExpr(n, expr)
}

func (n Node) Resilience() int{
    if n.DupFree(){
        return n.DupFreeMinFailures() - 1
    }

    qs := make([]map[GenericExpr]bool, 0)

    for q := range n.Quorums(){
        qs = append(qs, q)
    }

    return minHittingSet(qs) - 1
}

func DefNode(name string) Node {
    node := Node{}
    node.Name = name

    initialValue := 1.0
    node.ReadCapacity = &initialValue
    node.WriteCapacity = &initialValue

    return node
}


func DefNodeWithCapacity(name string, capacity *float64, readCapacity *float64, writeCapacity *float64, latency *time.Time) Node {
    node := Node{}
    node.Name = name

    if capacity == nil && readCapacity == nil && writeCapacity == nil {
        initialValue := 1.0
        node.ReadCapacity = &initialValue
        node.WriteCapacity = &initialValue

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

func (n Node) String() string {
    return n.Name
}



func (n Node) Quorums() chan map[GenericExpr]bool {
    chnl := make(chan map[GenericExpr]bool)

    go func() {
        chnl <- map[GenericExpr]bool{n : true}
        // Ensure that at the end of the loop we close the channel!
        close(chnl)
    }()

    return chnl
}

func (n Node) IsQuorum(xs map[GenericExpr]bool) bool {
    var found = false
    for  k := range xs {
        if n.String() == k.String() {
            found = true
            return found
        }
    }
    return found
}


func (n Node) Nodes() map[Node]bool {
   return map[Node]bool { n : true }
}

func (n Node) Dual() GenericExpr {
   return n
}

func (n Node) NumLeaves() int {
    return 1
}

func (n Node) DupFreeMinFailures() int {
    return 1
}

func (n Node) DupFree() bool {
    return len(n.Nodes()) == n.NumLeaves()
}

type GenericExpr interface {
   Add(expr GenericExpr) Or
   Multiply(expr GenericExpr) And
   Quorums() chan map[GenericExpr]bool
   IsQuorum(map[GenericExpr]bool) bool
   Nodes() map[Node]bool
   NumLeaves() int
   DupFreeMinFailures() int
   Resilience() int
   DupFree() bool
   String() string
   Expr() string
   GetEs() []GenericExpr
}

type Expr struct {
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

func (expr Or) GetEs() []GenericExpr {
    return expr.Es
}


func (expr Or) Expr() string {
    return "Or"
}

func (expr Or) NumLeaves() int {
    total := 0

    for _, e := range expr.Es {
        total += e.NumLeaves()
    }

    return total
}

func (expr Or) DupFreeMinFailures() int {
    total := 0

    for _, e := range expr.Es {
        total += e.DupFreeMinFailures()
    }

    return total
}

func (expr Or) Add(rhs GenericExpr) Or {
    return orExpr(expr, rhs)
}

func (expr Or) Multiply(rhs GenericExpr) And {
    return andExpr(expr, rhs)
}


func (expr Or) Quorums()  chan map[GenericExpr]bool {
    chnl := make(chan map[GenericExpr]bool)
    go func() {
        for _, e := range expr.Es {
            tmp := <- e.Quorums()
            chnl <- tmp

        }
        // Ensure that at the end of the loop we close the channel!
        close(chnl)
    }()
    return chnl
}

func (expr Or) Resilience() int{
    if expr.DupFree(){
        return expr.DupFreeMinFailures() - 1
    }

    qs := make([]map[GenericExpr]bool, 0)

    for q := range expr.Quorums(){
        qs = append(qs, q)
    }

    return minHittingSet(qs) - 1
}


func (expr Or) IsQuorum(xs map[GenericExpr]bool) bool {
    var found = false
    for  _, e := range expr.Es {
        if e.IsQuorum(xs) {
            found = true
            return found
        }
    }
    return found
}

func (expr Or) Nodes() map[Node]bool {
    var final = make(map[Node]bool)

    for _, e := range expr.Es {
        for n := range e.Nodes() {
            final[n] = true
        }
    }
    return final
}


func (expr Or) String() string {

    if len(expr.Es) == 0 {
        return "()"
    }
    var sb strings.Builder

    sb.WriteString("(")
    sb.WriteString(expr.Es[0].String())

    for _, v := range expr.Es[1:] {
        sb.WriteString(" + ")
        sb.WriteString(v.String())
    }

    sb.WriteString(")")
    return sb.String()
}



type And struct {
   Es []GenericExpr
}

func (expr And) GetEs() []GenericExpr {
    return expr.Es
}

func (expr And) Expr() string {
    return "And"
}
func (expr And) DupFree() bool {
    return len(expr.Nodes()) == expr.NumLeaves()
}
func (expr And) String() string {
    if len(expr.Es) == 0 {
        return "()"
    }
    var sb strings.Builder

    sb.WriteString("(")
    sb.WriteString(expr.Es[0].String())

    for _, v := range expr.Es[1:] {
        sb.WriteString(" * ")
        sb.WriteString(v.String())
    }

    sb.WriteString(")")

    return sb.String()
}

func (expr And) Add(rhs GenericExpr) Or {
    return orExpr(expr, rhs)
}

func (expr And) Multiply(rhs GenericExpr) And {
    return andExpr(expr, rhs)
}

func (expr Or) DupFree() bool {
    return len(expr.Nodes()) == expr.NumLeaves()
}

func (expr And) Quorums() chan map[GenericExpr]bool {
   chnl := make(chan map[GenericExpr]bool)
   flatQuorums := make([][]GenericExpr, 0)

   for _, e := range expr.Es {
       tmp := make(map[GenericExpr]bool, 0)

       for q := range e.Quorums(){
           tmp = mergeGenericExprSets(tmp, q)
       }
       flatQuorums = append(flatQuorums, exprMapToList(tmp))
   }

   go func() {
        for _, sets := range product(flatQuorums...){
            chnl <- exprListToMap(sets)
        }

      // Ensure that at the end of the loop we close the channel!
      close(chnl)
   }()
   return chnl
}

func (expr And) Resilience() int{
    if expr.DupFree(){
        return expr.DupFreeMinFailures() - 1
    }

    qs := make([]map[GenericExpr]bool, 0)

    for q := range expr.Quorums(){
       qs = append(qs, q)
    }

    return minHittingSet(qs) - 1
}
func (expr And) IsQuorum(xs map[GenericExpr]bool) bool {
    var found = true
    for  _, e := range expr.Es {
        if !e.IsQuorum(xs) {
            found = false
            return found
        }
    }
    return found
}


func (expr And) Nodes() map[Node]bool {
    var final = make(map[Node]bool)

    for _, e := range expr.Es {
        for n := range e.Nodes() {
            final[n] = true
        }
    }
    return final
}

func (expr And) Dual() GenericExpr {
    return &Or{expr.Es}
}


func (expr And) NumLeaves() int {
    total := 0

    for _, e := range expr.Es {
        total += e.NumLeaves()
    }

    return total
}

func (expr And) DupFreeMinFailures() int {
    var exprs = expr.Es
    var min = exprs[0].DupFreeMinFailures()

    for _, expr := range exprs {
        if min > expr.DupFreeMinFailures() {
            min = expr.DupFreeMinFailures()
        }
    }
    return min
}




func orExpr(lhs GenericExpr, rhs GenericExpr) Or {
    if lhs.Expr() == "Or" && rhs.Expr() == "Or"{
        return Or{append(lhs.GetEs(), rhs.GetEs()...)}
    }else if lhs.Expr() == "Or" {
        return Or{append(lhs.GetEs(), rhs)}
    }else if rhs.Expr() == "Or" {
        return Or{append([]GenericExpr{lhs}, rhs.GetEs()...)}
    }else{
        return Or{[]GenericExpr{lhs, rhs}}
    }
}

func andExpr(lhs GenericExpr, rhs GenericExpr) And {
    if lhs.Expr() == "And" && rhs.Expr() == "And"{
        return And{append(lhs.GetEs(), rhs.GetEs()...)}
    }else if lhs.Expr() == "And" {
        return And{append(lhs.GetEs(), rhs)}
    }else if rhs.Expr() == "And" {
        return And{append([]GenericExpr{lhs}, rhs.GetEs()...)}
    }else{
        return And{[]GenericExpr{lhs,rhs}}
    }
}


func mergeGenericExprSets(maps ...map[GenericExpr]bool) map[GenericExpr]bool {
    result := make(map[GenericExpr]bool)
    for _, m := range maps {
        for k, v := range m {
            result[k] = v
        }
    }
    return result
}
// Cartesian product of lists, see: https://www.programmersought.com/article/95476401483/
func product(sets ...[]GenericExpr) [][]GenericExpr {
    lens := func(i int) int { return len(sets[i]) }
    product := [][]GenericExpr{}
    for ix := make([]int, len(sets)); ix[0] < lens(0); nextIndex(ix, lens) {
        var r []GenericExpr

        for j, k := range ix {
            r = append(r, sets[j][k])
        }
        product = append(product, r)
    }
    return product
}

func nextIndex(ix []int, lens func(i int) int) {
    for j := len(ix) - 1; j >= 0; j-- {
        ix[j]++
        if j == 0 || ix[j] < lens(j) {
            return
        }
        ix[j] = 0
    }
}

func exprMapToList(input map[GenericExpr]bool) []GenericExpr{
    result:= make([]GenericExpr, 0)

    for k,_ := range input{
        result = append(result, k)
    }

    return result
}

func exprListToMap(input []GenericExpr) map[GenericExpr]bool{
    result:= make(map[GenericExpr]bool)

    for _,k := range input {
        result[k] = true
    }

    return result
}


func minHittingSet(sets []map[GenericExpr]bool) int {

    xVars := make(map[GenericExpr]float64)
    x := make([]float64, 0)
    constraints := make([][2]float64, 0)
    obj := make([][]float64, 0)
    pinf := math.Inf(1)
    simp := clp.NewSimplex()

    for _, xs := range sets {
        for k := range xs {
            xVars[k] = 1.0
        }
    }

    for range xVars {
        x = append(x, 1.0)
    }

    for range xVars {
        tmp := [][2]float64{{0, 1}}
        constraints = append(constraints, tmp...)
    }

    for _, xs := range sets {
        tmp := make([]float64, 0)
        tmp = append(tmp, 1)

        for k, _ := range xVars {
            if _, ok := xs[k]; ok {
                tmp = append(tmp, 1)
            }else{
                tmp = append(tmp, 0)
            }
        }
        tmp = append(tmp, pinf)
        obj = append(obj, tmp)
    }

    // Set up the optimization problem.

    simp.EasyLoadDenseProblem(
        x,
        constraints,
        obj)

    simp.SetOptimizationDirection(clp.Minimize)

    // Solve the optimization problem.
    simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
    soln := simp.PrimalColumnSolution()
    fmt.Println(soln)

    result := 0.0
    for _, v := range soln {
        result += v
    }
    return int(math.Round(result))
}