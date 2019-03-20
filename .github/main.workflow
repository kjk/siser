workflow "run go test on push" {
  on = "push"
  resolves = ["test"]
}

action "test" {
  uses = "kjk/siser/action-go-test@master"
}
