package reaper

import (
	"testing"

	testlogr "github.com/go-logr/logr/testing"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
	telemetryapi "github.com/k8ssandra/k8ssandra-operator/apis/telemetry/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/vector"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

func TestConfigureVector(t *testing.T) {
	telemetrySpec := &telemetryapi.TelemetrySpec{Vector: &telemetryapi.VectorSpec{Enabled: ptr.To(true)}}
	reaper := &api.Reaper{}
	reaper.Spec.Telemetry = telemetrySpec

	deployment := &v1.Deployment{}
	fakeDc := &cassdcapi.CassandraDatacenter{}

	logger := testlogr.NewTestLogger(t)
	configureVector(reaper, deployment, fakeDc, logger)

	assert.Equal(t, 1, len(deployment.Spec.Template.Spec.Containers))
	assert.Equal(t, "reaper-vector-agent", deployment.Spec.Template.Spec.Containers[0].Name)
	assert.Equal(t, resource.MustParse(vector.DefaultVectorCpuLimit), *deployment.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu())
	assert.Equal(t, resource.MustParse(vector.DefaultVectorCpuRequest), *deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu())
	assert.Equal(t, resource.MustParse(vector.DefaultVectorMemoryLimit), *deployment.Spec.Template.Spec.Containers[0].Resources.Limits.Memory())
	assert.Equal(t, resource.MustParse(vector.DefaultVectorMemoryRequest), *deployment.Spec.Template.Spec.Containers[0].Resources.Requests.Memory())
}
