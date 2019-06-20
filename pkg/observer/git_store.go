package observer

import (
	"os"
	"sync"
	"time"

	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

type GitStorage struct {
	repo    *git.Repository
	storage storage.Storer

	sync.Mutex
}

func NewGitStorage() (*GitStorage, error) {
	memoryStorage := memory.NewStorage()
	repo, err := git.Init(memoryStorage, nil)
	if err != nil {
		return nil, err
	}
	return &GitStorage{
		repo:    repo,
		storage: memoryStorage,
	}, nil
}

func (s *GitStorage) addAndCommit(name string) (string, error) {
	worktree, err := s.repo.Worktree()
	if err != nil {
		return "", err
	}
	if _, err := worktree.Add(name); err != nil {
		return "", err
	}
	hash, err := worktree.Commit("test commit", &git.CommitOptions{
		All: true,
		Author: &object.Signature{
			Name:  "Operator",
			Email: "operator@openshift.io",
			When:  time.Now(),
		},
		// TODO: Provide the CRD operator?
		Committer: &object.Signature{
			Name:  "Operator",
			Email: "operator@openshift.io",
			When:  time.Now(),
		},
	})
	if err != nil {
		return "", err
	}
	return hash.String(), err
}

func (s *GitStorage) updateFile(name string, content []byte) error {
	worktree, err := s.repo.Worktree()
	if err != nil {
		return err
	}

	// Simple "create file"
	if _, err := worktree.Filesystem.Lstat(name); err != nil {
		if os.IsNotExist(err) {
			f, err := worktree.Filesystem.Create(name)
			if err != nil {
				return err
			}
			if _, err := f.Write(content); err != nil {
				return err
			}
			return f.Close()
		}
	}

	// Updating existing file by replacing the original
	if err := worktree.Filesystem.Remove(name); err != nil {
		return err
	}
	return s.updateFile(name, content)
}

func (s *GitStorage) addConfigObject(obj interface{}) {
	s.Lock()
	defer s.Unlock()

	unstruct := obj.(*unstructured.Unstructured)
	objName := unstruct.GetName()
	objVersion := unstruct.GroupVersionKind()
	objBytes, err := runtime.Encode(unstructured.UnstructuredJSONScheme, unstruct)
	if err != nil {
		panic(err)
	}

	// s.updateFile()

	panic("implement me")

}

func (s *GitStorage) updateConfigObject(old, new interface{}) {
	s.Lock()
	defer s.Unlock()
	panic("implement me")
}

func (s *GitStorage) deleteConfigObject(obj interface{}) {
	s.Lock()
	defer s.Unlock()
	panic("implement me")
}

func (s *GitStorage) eventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    s.addConfigObject,
		UpdateFunc: s.updateConfigObject,
		DeleteFunc: s.deleteConfigObject,
	}
}
