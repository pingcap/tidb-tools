// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

const (
	// dm's http api version, define in https://github.com/pingcap/dm/blob/master/dm/proto/dmmaster.proto
	apiVersion = "v1alpha1"
)

func getDMTaskCfgURL(dmAddr, task string) string {
	return fmt.Sprintf("%s/apis/%s/subtasks/%s", dmAddr, apiVersion, task)
}

// getDMTaskCfg gets dm's sub task config
func getDMTaskCfg(dmAddr, task string) ([]*config.SubTaskConfig, error) {
	tr := &http.Transport{
		// TODO: support tls
		//TLSClientConfig: tlsCfg,
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest("GET", getDMTaskCfgURL(dmAddr, task), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	getSubTaskCfgResp := &pb.GetSubTaskCfgResponse{}
	err = json.Unmarshal(body, getSubTaskCfgResp)
	if err != nil {
		return nil, err
	}

	if !getSubTaskCfgResp.Result {
		return nil, errors.Errorf("fail to get sub task config from DM, %s", getSubTaskCfgResp.Msg)
	}

	subTaskCfgs := make([]*config.SubTaskConfig, 0, len(getSubTaskCfgResp.Cfgs))
	for _, cfgBytes := range getSubTaskCfgResp.Cfgs {
		subtaskCfg := &config.SubTaskConfig{}
		err = subtaskCfg.Decode(cfgBytes, false)
		if err != nil {
			return nil, err
		}
		subtaskCfg.To.Password = utils.DecryptOrPlaintext(subtaskCfg.To.Password)
		subtaskCfg.From.Password = utils.DecryptOrPlaintext(subtaskCfg.From.Password)
		subTaskCfgs = append(subTaskCfgs, subtaskCfg)
	}

	log.Info("dm sub task configs", zap.Reflect("cfgs", subTaskCfgs))
	return subTaskCfgs, nil
}
